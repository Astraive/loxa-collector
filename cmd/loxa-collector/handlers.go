package main

import (
	"bufio"
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/astraive/loxa-collector/internal/ingest"
	"github.com/astraive/loxa-collector/internal/validation"
)

func (s *collectorState) handleIngest(w http.ResponseWriter, r *http.Request) {
	s.metrics.requestsTotal.Add(1)

	if s.cfg.rateLimitEnabled && !s.rateLimiter.Allow() {
		s.metrics.requestsLimited.Add(1)
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusTooManyRequests, ingest.Response{Rejected: 1})
		return
	}

	if s.cfg.authEnabled && s.cfg.apiKey != "" {
		providedKey := r.Header.Get(s.cfg.apiKeyHeader)
		if subtle.ConstantTimeCompare([]byte(providedKey), []byte(s.cfg.apiKey)) != 1 {
			s.metrics.requestsAuthErr.Add(1)
			s.metrics.eventsRejected.Add(1)
			writeJSON(w, http.StatusUnauthorized, ingest.Response{Rejected: 1})
			return
		}
	}

	rawEvents, err := ingest.ParseEvents(r, s.cfg.maxBodyBytes)
	if err != nil {
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    "invalid request payload",
			"rejected": 1,
		})
		return
	}

	if len(rawEvents) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    "empty event payload",
			"rejected": 1,
		})
		return
	}

	if len(rawEvents) > s.cfg.maxEventsPerRequest {
		s.metrics.eventsRejected.Add(int64(len(rawEvents)))
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":    "max events exceeded",
			"rejected": len(rawEvents),
		})
		return
	}

	var resp ingest.Response
	for _, raw := range rawEvents {
		if !validation.IsJSONObject(raw) {
			resp.Invalid++
			s.metrics.eventsInvalid.Add(1)
			continue
		}

		if s.cfg.dedupeEnabled {
			eventID, ok := validation.ExtractStringPath(raw, s.cfg.dedupeKey)
			if ok && s.isDuplicate(eventID) {
				resp.Deduped++
				s.metrics.eventsDeduped.Add(1)
				continue
			}
		}

		if s.cfg.reliabilityMode == "spool" {
			if err := s.appendSpool(raw); err != nil {
				resp.Rejected++
				s.metrics.eventsRejected.Add(1)
				if isDiskFullErr(err) {
					s.diskHealthy.Store(false)
				}
				logJSON("error", "collector_spool_write_failed", map[string]any{"error": err.Error()})
				continue
			}
			s.enqueueDelivery(raw)
		} else {
			// Direct mode - processor handles delivery with built-in retry
			if err := s.ensureProcessor(); err != nil {
				resp.Rejected++
				s.metrics.eventsRejected.Add(1)
				s.sinkHealthy.Store(false)
				logJSON("error", "collector_pipeline_not_initialized", map[string]any{"error": err.Error()})
				continue
			}
			result := s.processor.Process(r.Context(), raw)
			if failures := result.Outcome.FailureCount(); failures > 0 {
				s.metrics.sinkWriteErrors.Add(int64(failures))
			}
			if result.Deduped {
				resp.Deduped++
				s.metrics.eventsDeduped.Add(1)
				continue
			}
			if result.Invalid {
				resp.Invalid++
				s.metrics.eventsInvalid.Add(1)
				continue
			}
			if result.Err != nil {
				resp.Rejected++
				s.metrics.eventsRejected.Add(1)
				s.sinkHealthy.Store(false)
				if s.cfg.reliabilityMode == "queue" {
					logJSON("error", "collector_kafka_enqueue_failed", map[string]any{"error": result.Err.Error()})
				} else {
					logJSON("error", "collector_sink_write_failed", map[string]any{"error": result.Err.Error()})
				}
				continue
			}
			s.sinkHealthy.Store(true)
		}

		resp.Accepted++
		s.metrics.eventsAccepted.Add(1)
		s.broadcastTail(raw)
	}

	status := http.StatusAccepted
	switch {
	case resp.Accepted == 0 && resp.Invalid > 0 && resp.Rejected == 0:
		status = http.StatusBadRequest
	case resp.Accepted == 0 && resp.Rejected > 0:
		status = http.StatusServiceUnavailable
	case resp.Accepted > 0 && (resp.Invalid > 0 || resp.Rejected > 0):
		status = http.StatusMultiStatus
	}

	writeJSON(w, status, resp)
}

func isDiskFullErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "no space left") || strings.Contains(s, "disk full")
}

func (s *collectorState) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *collectorState) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.isReady() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *collectorState) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.metricsHandler().ServeHTTP(w, r)
}

func (s *collectorState) handleTail(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := make(chan []byte, 128)
	s.addTailSubscriber(ch)
	defer s.removeTailSubscriber(ch)

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	bw := bufio.NewWriter(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case raw, ok := <-ch:
			if !ok {
				return
			}
			if _, err := bw.Write(raw); err != nil {
				return
			}
			if len(raw) == 0 || raw[len(raw)-1] != '\n' {
				if err := bw.WriteByte('\n'); err != nil {
					return
				}
			}
			if err := bw.Flush(); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *collectorState) handleIngestBatch(ctx context.Context, rawEvents [][]byte) (int, error) {
	accepted := 0

	for _, raw := range rawEvents {
		if len(raw) == 0 {
			continue
		}

		if !validation.IsJSONObject(raw) {
			s.metrics.eventsInvalid.Add(1)
			continue
		}

		if s.cfg.dedupeEnabled {
			eventID, ok := validation.ExtractStringPath(raw, s.cfg.dedupeKey)
			if ok && s.isDuplicate(eventID) {
				accepted++
				s.metrics.eventsAccepted.Add(1)
				s.metrics.eventsDeduped.Add(1)
				continue
			}
		}

		if s.cfg.reliabilityMode == "spool" {
			if err := s.appendSpool(raw); err != nil {
				if isDiskFullErr(err) {
					s.diskHealthy.Store(false)
				}
				logJSON("error", "collector_spool_write_failed", map[string]any{"error": err.Error()})
				continue
			}
			s.enqueueDelivery(raw)
		} else {
			if err := s.ensureProcessor(); err != nil {
				s.sinkHealthy.Store(false)
				logJSON("error", "collector_pipeline_not_initialized", map[string]any{"error": err.Error()})
				continue
			}
			result := s.processor.Process(ctx, raw)
			if failures := result.Outcome.FailureCount(); failures > 0 {
				s.metrics.sinkWriteErrors.Add(int64(failures))
			}
			if result.Deduped {
				s.metrics.eventsDeduped.Add(1)
				continue
			}
			if result.Invalid {
				s.metrics.eventsInvalid.Add(1)
				continue
			}
			if result.Err != nil {
				s.sinkHealthy.Store(false)
				if s.cfg.reliabilityMode == "queue" {
					logJSON("error", "collector_kafka_enqueue_failed", map[string]any{"error": result.Err.Error()})
				} else {
					logJSON("error", "collector_sink_write_failed", map[string]any{"error": result.Err.Error()})
				}
				continue
			}
			s.sinkHealthy.Store(true)
		}

		accepted++
		s.metrics.eventsAccepted.Add(1)
		s.broadcastTail(raw)
	}

	return accepted, nil
}

func (s *collectorState) addTailSubscriber(ch chan []byte) {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	if s.tailSubscribers == nil {
		s.tailSubscribers = make(map[chan []byte]struct{})
	}
	s.tailSubscribers[ch] = struct{}{}
}

func (s *collectorState) removeTailSubscriber(ch chan []byte) {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	if s.tailSubscribers == nil {
		return
	}
	delete(s.tailSubscribers, ch)
	close(ch)
}

func (s *collectorState) broadcastTail(raw []byte) {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	if len(s.tailSubscribers) == 0 {
		return
	}
	cp := append([]byte(nil), raw...)
	for ch := range s.tailSubscribers {
		select {
		case ch <- cp:
		default:
		}
	}
}
