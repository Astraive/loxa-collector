package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/astraive/loxa-collector/internal/ingest"
	"github.com/astraive/loxa-collector/internal/validation"
)

func (s *collectorState) handleIngest(w http.ResponseWriter, r *http.Request) {
	s.metrics.requestsTotal.Add(1)
	requestID := newIngestRequestID()

	if s.cfg.rateLimitEnabled && !s.rateLimiter.Allow() {
		s.metrics.requestsLimited.Add(1)
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusTooManyRequests, ingest.Response{
			RequestID:    requestID,
			Status:       ingest.StatusRejected,
			Rejected:     1,
			RetryAfterMS: s.cfg.retryAfterMS(),
			Reason:       "rate_limited",
			Error:        "rate_limited",
			Errors: []ingest.EventError{{
				Index:     0,
				Code:      "rate_limited",
				Message:   "collector rate limit exceeded",
				Retryable: true,
			}},
		})
		return
	}

	if !s.isAuthorized(r) {
		s.metrics.requestsAuthErr.Add(1)
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusUnauthorized, ingest.Response{
			RequestID: requestID,
			Status:    ingest.StatusRejected,
			Rejected:  1,
			Error:     "auth_failed",
			Reason:    "auth_failed",
			Errors: []ingest.EventError{{
				Index:     0,
				Code:      "auth_failed",
				Message:   "collector authentication failed",
				Retryable: false,
			}},
		})
		return
	}

	if !isSupportedIngestContentType(r.Header.Get("Content-Type")) {
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusUnsupportedMediaType, ingest.Response{
			RequestID: requestID,
			Status:    ingest.StatusRejected,
			Rejected:  1,
			Error:     "unsupported_content_type",
			Reason:    "unsupported_content_type",
			Errors: []ingest.EventError{{
				Index:     0,
				Code:      "unsupported_content_type",
				Message:   "content type must be application/json, application/x-ndjson, or application/ndjson",
				Retryable: false,
			}},
		})
		return
	}

	rawEvents, err := ingest.ParseEvents(r, s.cfg.maxBodyBytes)
	if err != nil {
		s.metrics.eventsRejected.Add(1)
		status := http.StatusBadRequest
		code := "invalid_request_payload"
		if strings.Contains(strings.ToLower(err.Error()), "max body bytes") {
			status = http.StatusRequestEntityTooLarge
			code = "payload_too_large"
		}
		writeJSON(w, status, ingest.Response{
			RequestID: requestID,
			Status:    ingest.StatusRejected,
			Rejected:  1,
			Error:     code,
			Reason:    code,
			Errors: []ingest.EventError{{
				Index:     0,
				Code:      code,
				Message:   err.Error(),
				Retryable: false,
			}},
		})
		return
	}

	if len(rawEvents) == 0 {
		writeJSON(w, http.StatusBadRequest, ingest.Response{
			RequestID: requestID,
			Status:    ingest.StatusInvalid,
			Rejected:  1,
			Error:     "empty_event_payload",
			Reason:    "empty_event_payload",
			Errors: []ingest.EventError{{
				Index:     0,
				Code:      "empty_event_payload",
				Message:   "empty event payload",
				Retryable: false,
			}},
		})
		return
	}

	if len(rawEvents) > s.cfg.maxEventsPerRequest {
		s.metrics.eventsRejected.Add(int64(len(rawEvents)))
		writeJSON(w, http.StatusRequestEntityTooLarge, ingest.Response{
			RequestID: requestID,
			Status:    ingest.StatusRejected,
			Rejected:  len(rawEvents),
			Error:     "max_events_exceeded",
			Reason:    "max_events_exceeded",
			Errors: []ingest.EventError{{
				Index:     0,
				Code:      "max_events_exceeded",
				Message:   "max events per request exceeded",
				Retryable: false,
			}},
		})
		return
	}

	totalBytes := int64(0)
	for _, raw := range rawEvents {
		totalBytes += int64(len(raw))
	}
	if gerr, release := s.acquireIngestCapacity(len(rawEvents), totalBytes); gerr != nil {
		s.metrics.requestsThrottled.Add(1)
		s.metrics.eventsRejected.Add(int64(len(rawEvents)))
		writeJSON(w, http.StatusTooManyRequests, ingest.Response{
			RequestID:    requestID,
			Status:       ingest.StatusRejected,
			Rejected:     len(rawEvents),
			RetryAfterMS: s.cfg.retryAfterMS(),
			Reason:       gerr.Code,
			Error:        gerr.Code,
			Errors: []ingest.EventError{{
				Index:     0,
				Code:      gerr.Code,
				Message:   gerr.Message,
				Retryable: gerr.Retryable,
			}},
		})
		return
	} else {
		defer release()
	}

	resp := ingest.Response{RequestID: requestID}
	for i, raw := range rawEvents {
		if !validation.IsJSONObject(raw) {
			s.metrics.eventsInvalid.Add(1)
			resp.AddInvalid(i, "", "expected_json_object", "event payload must be a JSON object")
			continue
		}

		eventID, _ := validation.ExtractStringPath(raw, "event_id")
		if reason, ok := s.validateEventContract(raw, s.cfg.schemaSchemaVersion, s.cfg.schemaEventVersion); ok {
			s.metrics.eventsInvalid.Add(1)
			resp.AddInvalid(i, eventID, reason, reason)
			continue
		}
		if prepared, gerr := s.prepareEvent(raw); gerr != nil {
			s.metrics.eventsInvalid.Add(1)
			resp.AddInvalid(i, eventID, gerr.Code, gerr.Message)
			continue
		} else {
			raw = prepared
		}

		if s.cfg.reliabilityMode == "spool" || s.cfg.reliabilityMode == "hybrid" {
			// For spool mode, deduplicate at ingest time to avoid unnecessary spooling
			if s.dedupeEnabled() {
				dedupeValue, ok := validation.ExtractStringPath(raw, s.cfg.dedupeKey)
				if ok && s.isDuplicate(dedupeValue) {
					s.metrics.eventsDeduped.Add(1)
					s.metrics.eventsAccepted.Add(1)
					resp.AddDuplicate(i, eventID)
					continue
				}
			}

			if err := s.appendSpool(raw); err != nil {
				s.metrics.eventsRejected.Add(1)
				resp.AddRejected(i, eventID, "spool_write_failed", err.Error(), true)
				if isDiskFullErr(err) {
					s.diskHealthy.Store(false)
				}
				logJSON("error", "collector_spool_write_failed", map[string]any{"error": err.Error()})
				continue
			}
			if s.cfg.reliabilityMode == "hybrid" && s.hybridQueueSink != nil {
				if err := s.hybridQueueSink.WriteEvent(r.Context(), raw, nil); err != nil {
					s.metrics.eventsRejected.Add(1)
					resp.AddRejected(i, eventID, "hybrid_queue_failed", err.Error(), true)
					continue
				}
			}
			s.enqueueDelivery(raw)
		} else {
			// Direct mode - processor handles delivery with deduplication
			if err := s.ensureProcessor(); err != nil {
				s.metrics.eventsRejected.Add(1)
				s.sinkHealthy.Store(false)
				logJSON("error", "collector_pipeline_not_initialized", map[string]any{"error": err.Error()})
				resp.AddRejected(i, eventID, "pipeline_not_initialized", err.Error(), true)
				continue
			}
			result := s.processor.Process(r.Context(), raw)
			if failures := result.Outcome.FailureCount(); failures > 0 {
				s.metrics.sinkWriteErrors.Add(int64(failures))
			}
			if result.Deduped {
				s.metrics.eventsDeduped.Add(1)
				s.metrics.eventsAccepted.Add(1)
				resp.AddDuplicate(i, eventID)
				continue
			}
			if result.Invalid {
				s.metrics.eventsInvalid.Add(1)
				message := "schema validation failed"
				if result.Err != nil {
					message = result.Err.Error()
				}
				resp.AddInvalid(i, eventID, "schema_invalid", message)
				continue
			}
			if result.Err != nil {
				s.metrics.eventsRejected.Add(1)
				resp.AddRejected(i, eventID, "delivery_failed", result.Err.Error(), true)
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

		resp.AddAccepted(i, eventID)
		s.metrics.eventsAccepted.Add(1)
		s.broadcastTail(raw)
	}

	resp.Finalize()
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

func (s *collectorState) validateEventContract(raw []byte, expectedSchemaVersion, expectedEventVersion string) (string, bool) {
	if !s.schemaValidationEnabled() {
		return "", false
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "invalid_json", true
	}
	if schemaVersion, _ := payload["schema_version"].(string); schemaVersion != "" && schemaVersion != expectedSchemaVersion {
		return "unsupported_schema_version", true
	}
	if eventVersion, _ := payload["event_version"].(string); eventVersion != "" && eventVersion != expectedEventVersion {
		return "unsupported_event_version", true
	}
	return "", false
}

func isDiskFullErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "no space left") || strings.Contains(s, "disk full")
}

func newIngestRequestID() string {
	return fmt.Sprintf("ing_%d", time.Now().UTC().UnixNano())
}

func (c collectorConfig) retryAfterMS() int {
	if c.retryInitialBackoff > 0 {
		return int(c.retryInitialBackoff / time.Millisecond)
	}
	return 1000
}

func isSupportedIngestContentType(header string) bool {
	value := strings.TrimSpace(header)
	if value == "" {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return false
	}
	switch strings.ToLower(mediaType) {
	case "application/json", "application/x-ndjson", "application/ndjson":
		return true
	default:
		return false
	}
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
	if !s.isAuthorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "auth_failed"})
		return
	}

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
	totalBytes := int64(0)
	for _, raw := range rawEvents {
		totalBytes += int64(len(raw))
	}
	if gerr, release := s.acquireIngestCapacity(len(rawEvents), totalBytes); gerr != nil {
		s.metrics.requestsThrottled.Add(1)
		return 0, gerr
	} else {
		defer release()
	}

	for _, raw := range rawEvents {
		if len(raw) == 0 {
			continue
		}

		if !validation.IsJSONObject(raw) {
			s.metrics.eventsInvalid.Add(1)
			continue
		}
		if reason, ok := s.validateEventContract(raw, s.cfg.schemaSchemaVersion, s.cfg.schemaEventVersion); ok {
			_ = reason
			s.metrics.eventsInvalid.Add(1)
			continue
		}
		if prepared, gerr := s.prepareEvent(raw); gerr != nil {
			s.metrics.eventsInvalid.Add(1)
			continue
		} else {
			raw = prepared
		}

		if s.dedupeEnabled() {
			eventID, ok := validation.ExtractStringPath(raw, s.cfg.dedupeKey)
			if ok && s.isDuplicate(eventID) {
				accepted++
				s.metrics.eventsAccepted.Add(1)
				s.metrics.eventsDeduped.Add(1)
				continue
			}
		}

		if s.cfg.reliabilityMode == "spool" || s.cfg.reliabilityMode == "hybrid" {
			if err := s.appendSpool(raw); err != nil {
				if isDiskFullErr(err) {
					s.diskHealthy.Store(false)
				}
				logJSON("error", "collector_spool_write_failed", map[string]any{"error": err.Error()})
				continue
			}
			if s.cfg.reliabilityMode == "hybrid" && s.hybridQueueSink != nil {
				if err := s.hybridQueueSink.WriteEvent(ctx, raw, nil); err != nil {
					return accepted, err
				}
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
