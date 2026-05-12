package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

func (s *collectorState) initReliability() error {
	if s.cfg.reliabilityMode != "spool" {
		return nil
	}
	s.reliabilityCtx, s.reliabilityCancel = context.WithCancel(context.Background())
	if err := os.MkdirAll(s.cfg.spoolDir, 0o755); err != nil {
		return fmt.Errorf("mkdir spool dir: %w", err)
	}

	spoolPath := filepath.Join(s.cfg.spoolDir, s.cfg.spoolFile)
	f, err := os.OpenFile(spoolPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open spool file: %w", err)
	}
	s.spoolFile = f

	posFilePath := spoolPath + ".pos"
	s.spoolPosFile = posFilePath

	if err := s.loadSpoolPosition(); err != nil {
		logJSON("warn", "spool_position_load_failed", map[string]any{"error": err.Error()})
		s.spoolProcessedPos = 0
	}

	if st, err := f.Stat(); err == nil {
		currentSize := st.Size()
		s.metrics.spoolBytes.Store(currentSize)
		if currentSize > s.cfg.maxSpoolBytes {
			s.spoolHealthy.Store(false)
		} else if s.spoolProcessedPos >= currentSize {
			s.metrics.spoolBytes.Store(0)
			s.spoolHealthy.Store(true)
		} else {
			s.spoolHealthy.Store(true)
		}
	}

	s.deliveryQueue = make(chan []byte, s.cfg.deliveryQueueSize)
	s.deliveryWG.Add(1)
	go s.deliveryWorker()

	if err := s.replaySpool(); err != nil {
		logJSON("error", "spool_replay_failed", map[string]any{"error": err.Error()})
	}

	return nil
}

func (s *collectorState) loadSpoolPosition() error {
	posData, err := os.ReadFile(s.spoolPosFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var pos struct {
		ProcessedPos int64 `json:"processed_pos"`
		EventCount   int64 `json:"event_count"`
	}
	if err := json.Unmarshal(posData, &pos); err != nil {
		return err
	}
	s.spoolProcessedPos = pos.ProcessedPos
	s.metrics.spoolReplayCount = pos.EventCount
	return nil
}

func (s *collectorState) saveSpoolPosition() error {
	if s.spoolPosFile == "" {
		return nil
	}
	pos := struct {
		ProcessedPos int64 `json:"processed_pos"`
		EventCount   int64 `json:"event_count"`
	}{
		ProcessedPos: s.spoolProcessedPos,
		EventCount:   s.metrics.spoolReplayCount,
	}
	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}
	tmpPath := s.spoolPosFile + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	_ = os.Remove(s.spoolPosFile)
	return os.Rename(tmpPath, s.spoolPosFile)
}

func (s *collectorState) closeReliability() {
	if s.reliabilityCancel != nil {
		s.reliabilityCancel()
		s.reliabilityCancel = nil
	}
	if s.deliveryQueue != nil {
		close(s.deliveryQueue)
		s.deliveryWG.Wait()
		s.deliveryQueue = nil
	}
	if s.spoolFile != nil {
		if err := s.spoolFile.Close(); err != nil {
			logJSON("error", "spool_close_failed", map[string]any{"error": err.Error()})
		}
		s.spoolFile = nil
	}
	if s.processor != nil {
		if err := s.processor.Close(); err != nil {
			logJSON("error", "processor_close_failed", map[string]any{"error": err.Error()})
		}
		s.processor = nil
	}
}

func (s *collectorState) appendSpool(raw []byte) error {
	s.spoolMu.Lock()
	defer s.spoolMu.Unlock()
	if s.spoolFile == nil {
		return errors.New("spool file is not initialized")
	}
	if _, err := s.spoolFile.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	n, err := s.spoolFile.Write(append(append([]byte(nil), raw...), '\n'))
	if err != nil {
		return err
	}
	if s.cfg.spoolFsync {
		if err := s.spoolFile.Sync(); err != nil {
			return err
		}
	}

	total := s.metrics.spoolBytes.Add(int64(n))
	s.spoolHealthy.Store(total <= s.cfg.maxSpoolBytes)
	return nil
}

func (s *collectorState) enqueueDelivery(raw []byte) {
	cp := append([]byte(nil), raw...)
	select {
	case s.deliveryQueue <- cp:
	default:
		s.metrics.sinkWriteErrors.Add(1)
		logJSON("warn", "delivery_queue_full_dropping_event", nil)
	}
}

func (s *collectorState) deliveryWorker() {
	defer s.deliveryWG.Done()
	for raw := range s.deliveryQueue {
		s.processSpoolEvent(raw)
	}
}

func (s *collectorState) processSpoolEvent(raw []byte) {
	if err := s.ensureProcessor(); err != nil {
		s.metrics.sinkWriteErrors.Add(1)
		s.sinkHealthy.Store(false)
		logJSON("error", "collector_pipeline_not_initialized", map[string]any{"error": err.Error()})
		return
	}

	ctx := s.reliabilityCtx
	if ctx == nil {
		ctx = context.Background()
	}
	result := s.processor.Process(ctx, raw)
	if failures := result.Outcome.FailureCount(); failures > 0 {
		s.metrics.sinkWriteErrors.Add(int64(failures))
	}

	if result.Err != nil {
		s.sinkHealthy.Store(false)
		logJSON("error", "spool_delivery_failed", map[string]any{"error": result.Err.Error()})
		return
	}

	s.sinkHealthy.Store(true)
	s.markSpoolDelivered(raw)
}

func (s *collectorState) markSpoolDelivered(raw []byte) {
	s.spoolMu.Lock()
	defer s.spoolMu.Unlock()

	if s.spoolFile == nil {
		return
	}

	currentSize, err := s.spoolFile.Seek(0, io.SeekEnd)
	if err != nil {
		logJSON("error", "spool_truncate_seek_failed", map[string]any{"error": err.Error()})
		return
	}

	s.spoolProcessedPos += int64(len(raw) + 1)
	if s.spoolProcessedPos < currentSize {
		s.metrics.spoolBytes.Store(currentSize - s.spoolProcessedPos)
		if err := s.saveSpoolPosition(); err != nil {
			logJSON("error", "spool_position_save_failed", map[string]any{"error": err.Error()})
		}
		return
	}

	if err := s.spoolFile.Truncate(0); err != nil {
		logJSON("error", "spool_truncate_failed", map[string]any{"error": err.Error()})
		return
	}
	if _, err := s.spoolFile.Seek(0, io.SeekStart); err != nil {
		logJSON("error", "spool_rewind_failed", map[string]any{"error": err.Error()})
		return
	}
	s.spoolProcessedPos = 0
	s.metrics.spoolBytes.Store(0)
	if err := s.saveSpoolPosition(); err != nil {
		logJSON("error", "spool_position_save_failed", map[string]any{"error": err.Error()})
	}
	if currentSize > 0 {
		s.metrics.spoolBytes.Store(0)
	}
}

func (s *collectorState) replaySpool() error {
	if s.spoolFile == nil {
		return nil
	}

	fileInfo, err := s.spoolFile.Stat()
	if err != nil {
		return err
	}
	currentSize := fileInfo.Size()

	if s.spoolProcessedPos >= currentSize {
		if currentSize > 0 {
			s.spoolMu.Lock()
			if err := s.spoolFile.Truncate(0); err == nil {
				_, _ = s.spoolFile.Seek(0, io.SeekStart)
				s.spoolProcessedPos = 0
				_ = s.saveSpoolPosition()
			}
			s.spoolMu.Unlock()
		}
		s.metrics.spoolBytes.Store(0)
		logJSON("info", "spool_already_processed", map[string]any{
			"processed_pos": s.spoolProcessedPos,
			"current_size":  currentSize,
		})
		return nil
	}

	if s.spoolProcessedPos > 0 {
		if _, err := s.spoolFile.Seek(s.spoolProcessedPos, io.SeekStart); err != nil {
			return err
		}
	} else {
		if _, err := s.spoolFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}

	replayCount := int64(0)
	sc := bufio.NewScanner(s.spoolFile)
	buf := make([]byte, 0, 1024*1024)
	sc.Buffer(buf, math.MaxInt32)

	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 || !json.Valid(line) {
			continue
		}
		s.enqueueDelivery(line)
		replayCount++
	}

	s.metrics.spoolReplayCount += replayCount

	logJSON("info", "spool_replay_completed", map[string]any{
		"replayed":    replayCount,
		"from_pos":    s.spoolProcessedPos,
		"total_count": s.metrics.spoolReplayCount,
	})

	_, _ = s.spoolFile.Seek(0, io.SeekEnd)
	return nil
}

type spoolDeliveryResult struct {
	Success bool
	Size    int
}
