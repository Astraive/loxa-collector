package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/astraive/loxa-collector/internal/ingest"
	storagepath "github.com/astraive/loxa-collector/internal/storage"
	"github.com/astraive/loxa-go"
	"golang.org/x/time/rate"
)

type fakeSink struct {
	events [][]byte
	fail   bool
}

func (s *fakeSink) Name() string { return "fake" }
func (s *fakeSink) WriteEvent(_ context.Context, encoded []byte, _ *loxa.Event) error {
	if s.fail {
		return context.DeadlineExceeded
	}
	cp := make([]byte, len(encoded))
	copy(cp, encoded)
	s.events = append(s.events, cp)
	return nil
}
func (s *fakeSink) Flush(_ context.Context) error { return nil }
func (s *fakeSink) Close(_ context.Context) error { return nil }

type errSink struct {
	err error
}

func (s errSink) Name() string { return "err" }
func (s errSink) WriteEvent(context.Context, []byte, *loxa.Event) error {
	return s.err
}
func (s errSink) Flush(context.Context) error { return nil }
func (s errSink) Close(context.Context) error { return nil }

func testCollectorConfig() collectorConfig {
	return runtimeConfigFromFile(defaultFileConfig())
}

func TestParseEventsJSONArray(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`[{"event":"a"},{"event":"b"}]`))
	events, err := ingest.ParseEvents(req, 1024)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestParseEventsWrappedEvents(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"events":[{"event":"a"},{"event":"b"}]}`))
	events, err := ingest.ParseEvents(req, 1024)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestParseEventsNDJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader("{\"event\":\"a\"}\n{\"event\":\"b\"}\n"))
	events, err := ingest.ParseEvents(req, 1024)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestHandleIngestAuthFailure(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.authEnabled = true
	cfg.apiKey = "secret"
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`[{"event":"a"}]`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleIngestPartialSuccess(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`[{"event":"a"},123]`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rec.Code)
	}
	var out ingest.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Accepted != 1 || out.Invalid != 1 || out.Rejected != 0 {
		t.Fatalf("unexpected response: %+v", out)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected 1 persisted event, got %d", len(sink.events))
	}
}

func TestHandleIngestSingleJSONObject(t *testing.T) {
	sink := &fakeSink{}
	state := &collectorState{
		cfg:         testCollectorConfig(),
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"a"}`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(sink.events))
	}
}

func TestHandleIngestInvalidJSON(t *testing.T) {
	sink := &fakeSink{}
	state := &collectorState{
		cfg:         testCollectorConfig(),
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleIngestNonObjectJSON(t *testing.T) {
	sink := &fakeSink{}
	state := &collectorState{
		cfg:         testCollectorConfig(),
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`123`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleIngestMaxBodyExceeded(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.maxBodyBytes = 4
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"too-big"}`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleIngestMaxEventsExceeded(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.maxEventsPerRequest = 1
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`[{"event":"a"},{"event":"b"}]`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestHandleIngestAPIPass(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.authEnabled = true
	cfg.apiKey = "secret"
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"a"}`))
	req.Header.Set(cfg.apiKeyHeader, "secret")
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
}

func TestHandleIngestRateLimit(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.rateLimitEnabled = true
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1), 1),
	}
	state.ready.Store(true)

	first := httptest.NewRecorder()
	state.handleIngest(first, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"a"}`)))
	second := httptest.NewRecorder()
	state.handleIngest(second, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"b"}`)))

	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", second.Code)
	}
}

func TestHandleIngestSinkWriteError(t *testing.T) {
	state := &collectorState{
		cfg:         testCollectorConfig(),
		ingestSink:  errSink{err: context.DeadlineExceeded},
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"a"}`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleIngestDiskFullFlipsReadiness(t *testing.T) {
	state := &collectorState{
		cfg:         testCollectorConfig(),
		ingestSink:  errSink{err: errors.New("disk full")},
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)
	state.sinkHealthy.Store(true)
	state.spoolHealthy.Store(true)
	state.diskHealthy.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"a"}`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if state.effectiveDiskHealthy() {
		t.Fatalf("expected disk health false after disk full")
	}
}

func TestHandleMetricsIncrements(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.authEnabled = true
	cfg.apiKey = "secret"
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1), 1),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"a"}`))
	req.Header.Set(cfg.apiKeyHeader, "secret")
	state.handleIngest(httptest.NewRecorder(), req)
	state.handleIngest(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"b"}`)))
	badReq := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"c"}`))
	badReq.Header.Set(cfg.apiKeyHeader, "wrong")
	state.handleIngest(httptest.NewRecorder(), badReq)

	rec := httptest.NewRecorder()
	state.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"loxa_collector_requests_total 3",
		"loxa_collector_events_accepted_total 1",
		"loxa_collector_requests_limited_total 2",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q in %q", want, body)
		}
	}
}

func TestHandleMetricsPrometheusFormat(t *testing.T) {
	state := &collectorState{
		cfg:         testCollectorConfig(),
		ingestSink:  &fakeSink{},
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	state.handleMetrics(rec, req)

	body := rec.Body.String()
	for _, want := range []string{
		"# HELP loxa_collector_requests_total",
		"# TYPE loxa_collector_spool_bytes gauge",
		"loxa_collector_ready 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected metrics body to include %q, got: %s", want, body)
		}
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Fatalf("expected prometheus content type, got %q", got)
	}
}

func TestHandleIngestSpoolModeAcceptsAndAsyncFailsToSink(t *testing.T) {
	cfg := testCollectorConfig()
	cfg.reliabilityMode = "spool"
	cfg.spoolDir = t.TempDir()
	cfg.maxSpoolBytes = 1024 * 1024
	cfg.spoolFsync = true
	cfg.retryEnabled = true
	cfg.retryMaxAttempts = 1
	cfg.retryInitialBackoff = time.Millisecond
	cfg.retryMaxBackoff = time.Millisecond
	cfg.dlqEnabled = true
	cfg.dlqPath = filepath.Join(t.TempDir(), "dlq.ndjson")

	state := &collectorState{
		cfg:          cfg,
		ingestSink:   errSink{err: context.DeadlineExceeded},
		rateLimiter:  rate.NewLimiter(rate.Limit(1000), 1000),
		rng:          randSourceForTests(),
		dedupeSeenAt: make(map[string]time.Time),
	}
	state.ready.Store(true)
	state.sinkHealthy.Store(true)
	state.spoolHealthy.Store(true)
	state.diskHealthy.Store(true)
	if err := state.initReliability(); err != nil {
		t.Fatalf("init reliability: %v", err)
	}
	defer state.closeReliability()

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event_id":"evt-1","event":"a"}`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if state.metrics.sinkWriteErrors.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if state.metrics.sinkWriteErrors.Load() == 0 {
		t.Fatalf("expected async sink errors to be recorded")
	}
	if state.metrics.spoolBytes.Load() == 0 {
		t.Fatalf("expected spool bytes > 0")
	}
}

func TestHandleIngestDedupe(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.dedupeEnabled = true
	cfg.dedupeKey = "event_id"
	cfg.dedupeWindow = time.Minute

	state := &collectorState{
		cfg:          cfg,
		ingestSink:   sink,
		rateLimiter:  rate.NewLimiter(rate.Limit(1000), 1000),
		dedupeSeenAt: make(map[string]time.Time),
	}
	state.ready.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`[{"event_id":"evt-1","event":"a"},{"event_id":"evt-1","event":"a"}]`))
	rec := httptest.NewRecorder()
	state.handleIngest(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected 1 sink write, got %d", len(sink.events))
	}
	if state.metrics.eventsDeduped.Load() != 1 {
		t.Fatalf("expected deduped metric 1, got %d", state.metrics.eventsDeduped.Load())
	}
}

func TestHandleIngestQueueModeAcceptsRawNonObject(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.reliabilityMode = "queue"
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	rec := httptest.NewRecorder()
	state.handleIngest(rec, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader("123")))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(sink.events) != 0 {
		t.Fatalf("expected invalid payload to be rejected before queueing, got %d sink writes", len(sink.events))
	}
}

func TestHandleIngestQueueModeSinkFailure(t *testing.T) {
	sink := &fakeSink{fail: true}
	cfg := testCollectorConfig()
	cfg.reliabilityMode = "queue"
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)
	state.sinkHealthy.Store(true)

	rec := httptest.NewRecorder()
	state.handleIngest(rec, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"event":"a"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d body=%s", rec.Code, rec.Body.String())
	}
	if state.metrics.sinkWriteErrors.Load() != 1 {
		t.Fatalf("expected sink write error metric, got %d", state.metrics.sinkWriteErrors.Load())
	}
	if state.effectiveSinkHealthy() {
		t.Fatalf("expected sink health to flip false")
	}
}

func randSourceForTests() *rand.Rand {
	return rand.New(rand.NewSource(1))
}

func TestExecuteCollectorCLIRunCommandParsesConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collector.yaml")
	raw := `
collector:
  addr: ":9191"
`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	called := false
	var got collectorConfig
	err := executeCollectorCLI([]string{"run", "-c", path}, func(cfg collectorConfig) error {
		called = true
		got = cfg
		return nil
	}, func([]string) error {
		t.Fatalf("config command should not be called")
		return nil
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !called {
		t.Fatalf("expected run function to be called")
	}
	if got.addr != ":9191" {
		t.Fatalf("expected run config addr :9191, got %q", got.addr)
	}
}

func TestExecuteCollectorCLIDispatchesConfigPrint(t *testing.T) {
	var got []string
	err := executeCollectorCLI([]string{"config", "print", "-c", "cfg.yaml"}, func(collectorConfig) error {
		t.Fatalf("run command should not be called")
		return nil
	}, func(args []string) error {
		got = append([]string(nil), args...)
		return nil
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if strings.Join(got, " ") != "print -c cfg.yaml" {
		t.Fatalf("unexpected config args: %v", got)
	}
}

func TestExecuteCollectorCLIDispatchesConfigValidate(t *testing.T) {
	var got []string
	err := executeCollectorCLI([]string{"config", "validate", "-c", "cfg.yaml"}, func(collectorConfig) error {
		t.Fatalf("run command should not be called")
		return nil
	}, func(args []string) error {
		got = append([]string(nil), args...)
		return nil
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if strings.Join(got, " ") != "validate -c cfg.yaml" {
		t.Fatalf("unexpected config args: %v", got)
	}
}

func TestBuildMuxSkipsMetricsWhenDisabled(t *testing.T) {
	sink := &fakeSink{}
	cfg := testCollectorConfig()
	cfg.metricsPrometheus = false
	state := &collectorState{
		cfg:         cfg,
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, cfg.metricsPath, nil)
	buildMux(state).ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when metrics disabled, got %d", rec.Code)
	}
}

func TestTailStreamsAcceptedEvents(t *testing.T) {
	sink := &fakeSink{}
	state := &collectorState{
		cfg:         testCollectorConfig(),
		ingestSink:  sink,
		rateLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
	}
	state.ready.Store(true)

	srv := httptest.NewServer(buildMux(state))
	defer srv.Close()

	tailResp, err := http.Get(srv.URL + "/tail")
	if err != nil {
		t.Fatalf("open tail stream: %v", err)
	}
	defer tailResp.Body.Close()

	done := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(tailResp.Body).ReadString('\n')
		done <- strings.TrimSpace(line)
	}()

	postResp, err := http.Post(srv.URL+"/ingest", "application/json", strings.NewReader(`{"event":"tail.event"}`))
	if err != nil {
		t.Fatalf("post ingest: %v", err)
	}
	io.Copy(io.Discard, postResp.Body)
	postResp.Body.Close()

	select {
	case line := <-done:
		if line != `{"event":"tail.event"}` {
			t.Fatalf("unexpected tail line: %q", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for tail event")
	}
}

func TestParquetExportPathConvention(t *testing.T) {
	ts := time.Date(2026, time.March, 9, 14, 5, 6, 123456789, time.UTC)
	got := filepath.ToSlash(storagepath.LocalParquetExportPath("exports", "analytics.events", ts))
	want := "exports/table=analytics_events/date=2026-03-09/hour=14/part-1773065106123456789.parquet"
	if got != want {
		t.Fatalf("unexpected export path:\nwant: %s\ngot:  %s", want, got)
	}
}
