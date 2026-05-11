package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	collectorconfig "github.com/astraive/loxa-collector/internal/config"
	"github.com/astraive/loxa-collector/internal/ingest"
	collectormetrics "github.com/astraive/loxa-collector/internal/metrics"
	processing "github.com/astraive/loxa-collector/internal/processing"
	storagepath "github.com/astraive/loxa-collector/internal/storage"
	"github.com/astraive/loxa-collector/internal/validation"
	"github.com/astraive/loxa-go"
	"github.com/astraive/loxa-go/sinks/duckdb"
	kafkasink "github.com/astraive/loxa-go/sinks/kafka"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
)

type collectorConfig struct {
	readHeaderTimeout       time.Duration
	addr                    string
	shutdownTimeout         time.Duration
	maxBodyBytes            int64
	maxEventsPerRequest     int
	authEnabled             bool
	apiKeyHeader            string
	apiKey                  string
	rateLimitEnabled        bool
	duckDBPath              string
	duckDBDriver            string
	duckDBTable             string
	duckDBRawColumn         string
	duckDBStoreRaw          bool
	duckDBCheckpointOnStop  bool
	duckDBMaxOpenConns      int
	duckDBMaxIdleConns      int
	duckDBBatchSize         int
	duckDBFlushInterval     time.Duration
	duckDBWriterLoop        bool
	duckDBWriterQueueSize   int
	duckDBCheckpointIntvl   time.Duration
	duckDBExportEnabled     bool
	duckDBExportFormat      string
	duckDBExportInterval    time.Duration
	duckDBExportPath        string
	duckDBSchema            map[string]string
	duckDBColumnTypes       map[string]string
	ingestPath              string
	healthPath              string
	readyPath               string
	metricsPath             string
	storagePrimary          string
	rateLimitRPS            float64
	rateLimitBurst          int
	loggingLevel            string
	loggingFormat           string
	metricsPrometheus       bool
	reliabilityMode         string
	spoolDir                string
	spoolFile               string
	maxSpoolBytes           int64
	spoolFsync              bool
	deliveryQueueSize       int
	retryEnabled            bool
	retryMaxAttempts        int
	retryInitialBackoff     time.Duration
	retryMaxBackoff         time.Duration
	retryJitter             bool
	dlqEnabled              bool
	dlqPath                 string
	fanoutOutputs           []collectorFanoutOutput
	deliveryPolicy          string
	fallbackEnabled         bool
	fallbackOnPrimaryFail   bool
	fallbackOnSecondaryFail bool
	fallbackOnPolicyFail    bool
	dlqOnPrimaryFail        bool
	dlqOnSecondaryFail      bool
	dlqOnFallbackFail       bool
	dlqOnPolicyFail         bool
	dedupeEnabled           bool
	dedupeKey               string
	dedupeWindow            time.Duration
	dedupeBackend           string
	kafkaBrokers            []string
	kafkaTopic              string
	workerConsumerGroup     string
	workerPollTimeout       time.Duration
	schemaMode              string
	schemaSchemaVersion     string
	schemaEventVersion      string
	schemaQuarantinePath    string
	schemaRegistry          []collectorconfig.SchemaRegistryEntry
}

type collectorMetrics struct {
	requestsTotal   atomic.Int64
	requestsAuthErr atomic.Int64
	requestsLimited atomic.Int64
	eventsAccepted  atomic.Int64
	eventsInvalid   atomic.Int64
	eventsRejected  atomic.Int64
	eventsDeduped   atomic.Int64
	sinkWriteErrors atomic.Int64
	spoolBytes      atomic.Int64
}

type collectorState struct {
	cfg            collectorConfig
	ingestSink     loxa.Sink
	secondarySinks []namedSink
	fallbackSink   *namedSink
	ready          atomic.Bool
	sinkHealthy    atomic.Bool
	spoolHealthy   atomic.Bool
	diskHealthy    atomic.Bool
	rateLimiter    *rate.Limiter
	metrics        collectorMetrics
	rng            *rand.Rand
	spoolFile      *os.File
	spoolMu        sync.Mutex
	deliveryQueue  chan []byte
	deliveryStop   chan struct{}
	deliveryWG     sync.WaitGroup
	metricsInit    sync.Once
	metricsHTTP    http.Handler
	dedupeMu       sync.Mutex
	dedupeSeenAt   map[string]time.Time
	processorMu    sync.Mutex
	processor      *processing.Processor
}

var version = "dev"

func main() {
	if err := executeCollectorCLI(os.Args[1:], runCollector, configCommand); err != nil {
		log.Fatalf("collector: %v", err)
	}
}

func executeCollectorCLI(args []string, runFn func(collectorConfig) error, configFn func([]string) error) error {
	if len(args) > 0 {
		switch args[0] {
		case "config":
			if err := configFn(args[1:]); err != nil {
				return fmt.Errorf("config: %w", err)
			}
			return nil
		case "run":
			args = args[1:]
		case "version", "-v", "--version":
			fmt.Println("loxa-collector version", version)
			return nil
		default:
			if !strings.HasPrefix(args[0], "-") {
				return fmt.Errorf("unknown command %q", args[0])
			}
		}
	}

	cfg, err := loadCollectorConfigFromArgs(args)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if err := runFn(cfg); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	return nil
}

func runCollector(cfg collectorConfig) error {
	var (
		db             *sql.DB
		err            error
		sink           loxa.Sink
		secondarySinks []namedSink
		fallbackSink   *namedSink
		fanoutDBs      []*sql.DB
		schedulersStop chan struct{}
		schedWG        sync.WaitGroup
	)

	if cfg.reliabilityMode == "queue" {
		sink, err = kafkasink.New(kafkasink.Config{
			Brokers: cfg.kafkaBrokers,
			Topic:   cfg.kafkaTopic,
		})
		if err != nil {
			return fmt.Errorf("failed to create kafka sink: %w", err)
		}
	} else {
		db, err = sql.Open(cfg.duckDBDriver, cfg.duckDBPath)
		if err != nil {
			return fmt.Errorf("failed to open duckdb: %w", err)
		}
		defer db.Close()
		db.SetMaxOpenConns(cfg.duckDBMaxOpenConns)
		db.SetMaxIdleConns(cfg.duckDBMaxIdleConns)

		if err := ensureSchema(db, cfg); err != nil {
			return fmt.Errorf("failed to initialize schema: %w", err)
		}

		sink, err = duckdb.New(duckdb.Config{
			DB:              db,
			Driver:          cfg.duckDBDriver,
			Table:           cfg.duckDBTable,
			StoreRaw:        cfg.duckDBStoreRaw,
			RawColumn:       cfg.duckDBRawColumn,
			Schema:          cfg.duckDBSchema,
			BatchSize:       cfg.duckDBBatchSize,
			FlushInterval:   cfg.duckDBFlushInterval,
			WriterLoop:      cfg.duckDBWriterLoop,
			WriterQueueSize: cfg.duckDBWriterQueueSize,
		})
		if err != nil {
			return fmt.Errorf("failed to create duckdb sink: %w", err)
		}
		secondarySinks, fallbackSink, fanoutDBs, err = createFanoutSinks(cfg)
		if err != nil {
			return err
		}
		defer func() {
			for _, fanoutDB := range fanoutDBs {
				_ = fanoutDB.Close()
			}
		}()

		schedulersStop = make(chan struct{})
		if cfg.duckDBCheckpointIntvl > 0 {
			schedWG.Add(1)
			go runPeriodicCheckpoint(db, cfg.duckDBCheckpointIntvl, schedulersStop, &schedWG)
		}
		if cfg.duckDBExportEnabled {
			schedWG.Add(1)
			go runPeriodicExport(db, cfg, schedulersStop, &schedWG)
		}
	}

	rateLimiter := rate.NewLimiter(rate.Inf, 0)
	if cfg.rateLimitEnabled {
		rateLimiter = rate.NewLimiter(rate.Limit(cfg.rateLimitRPS), cfg.rateLimitBurst)
	}

	state := &collectorState{
		cfg:            cfg,
		ingestSink:     sink,
		secondarySinks: secondarySinks,
		fallbackSink:   fallbackSink,
		rateLimiter:    rateLimiter,
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
		dedupeSeenAt:   make(map[string]time.Time),
	}
	state.ready.Store(true)
	state.sinkHealthy.Store(true)
	state.spoolHealthy.Store(true)
	state.diskHealthy.Store(true)

	if err := state.initReliability(); err != nil {
		return err
	}
	defer state.closeReliability()
	if cfg.reliabilityMode != "queue" {
		state.processor, err = processing.New(processing.Config{
			DeliveryPolicy:          cfg.deliveryPolicy,
			RetryEnabled:            cfg.retryEnabled,
			RetryMaxAttempts:        cfg.retryMaxAttempts,
			RetryInitialBackoff:     cfg.retryInitialBackoff,
			RetryMaxBackoff:         cfg.retryMaxBackoff,
			RetryJitter:             cfg.retryJitter,
			FallbackEnabled:         cfg.fallbackEnabled,
			FallbackOnPrimaryFail:   cfg.fallbackOnPrimaryFail,
			FallbackOnSecondaryFail: cfg.fallbackOnSecondaryFail,
			FallbackOnPolicyFail:    cfg.fallbackOnPolicyFail,
			DLQEnabled:              cfg.dlqEnabled,
			DLQPath:                 cfg.dlqPath,
			DLQOnPrimaryFail:        cfg.dlqOnPrimaryFail,
			DLQOnSecondaryFail:      cfg.dlqOnSecondaryFail,
			DLQOnFallbackFail:       state.cfg.dlqOnFallbackFail,
			DLQOnPolicyFail:         state.cfg.dlqOnPolicyFail,
			OnDiskFull: func() {
				state.diskHealthy.Store(false)
			},
			Schema: processing.SchemaConfig{
				Mode:           state.cfg.schemaMode,
				SchemaVersion:  state.cfg.schemaSchemaVersion,
				EventVersion:   state.cfg.schemaEventVersion,
				QuarantinePath: state.cfg.schemaQuarantinePath,
				Registry:       state.convertSchemaRegistry(),
			},
		}, sink, secondarySinks, fallbackSink, state.rng)
		if err != nil {
			return err
		}
	}

	mux := buildMux(state)

	server := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: cfg.readHeaderTimeout,
	}

	go func() {
		logJSON("info", "collector_start", map[string]any{
			"addr":             cfg.addr,
			"reliability_mode": cfg.reliabilityMode,
			"duckdb_path":      cfg.duckDBPath,
			"kafka_topic":      cfg.kafkaTopic,
		})
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %s", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logJSON("info", "collector_shutdown_begin", nil)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()

	state.ready.Store(false)
	if err := server.Shutdown(ctx); err != nil {
		logJSON("error", "collector_http_shutdown_failed", map[string]any{"error": err.Error()})
	}

	for _, sink := range state.sinksForShutdown() {
		if err := sink.Sink.Flush(ctx); err != nil {
			logJSON("error", "collector_sink_flush_failed", map[string]any{"sink": sink.Name, "error": err.Error()})
		}
		if err := sink.Sink.Close(ctx); err != nil {
			logJSON("error", "collector_sink_close_failed", map[string]any{"sink": sink.Name, "error": err.Error()})
		}
	}

	if db != nil && cfg.duckDBCheckpointOnStop {
		if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
			logJSON("error", "collector_duckdb_checkpoint_failed", map[string]any{"error": err.Error()})
		}
	}
	if schedulersStop != nil {
		close(schedulersStop)
		schedWG.Wait()
	}

	logJSON("info", "collector_shutdown_complete", nil)
	return nil
}

func buildMux(state *collectorState) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+state.cfg.ingestPath, state.handleIngest)
	mux.HandleFunc("GET "+state.cfg.healthPath, state.handleHealth)
	mux.HandleFunc("GET "+state.cfg.readyPath, state.handleReady)
	if state.cfg.metricsPrometheus {
		mux.Handle("GET "+state.cfg.metricsPath, state.metricsHandler())
	}
	return mux
}

func ensureSchema(db *sql.DB, cfg collectorConfig) error {
	columns := make([]string, 0, len(cfg.duckDBSchema)+1)
	for col, path := range cfg.duckDBSchema {
		if typ, ok := cfg.duckDBColumnTypes[path]; ok {
			columns = append(columns, fmt.Sprintf("%s %s", col, typ))
		} else {
			columns = append(columns, fmt.Sprintf("%s TEXT", col))
		}
	}
	if cfg.duckDBStoreRaw {
		columns = append(columns, fmt.Sprintf("%s TEXT", cfg.duckDBRawColumn))
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", cfg.duckDBTable, strings.Join(columns, ", "))
	_, err := db.Exec(query)
	return err
}

func (s *collectorState) handleIngest(w http.ResponseWriter, r *http.Request) {
	s.metrics.requestsTotal.Add(1)

	if s.cfg.rateLimitEnabled && !s.rateLimiter.Allow() {
		s.metrics.requestsLimited.Add(1)
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusTooManyRequests, ingest.Response{Rejected: 1})
		return
	}

	if s.cfg.authEnabled && s.cfg.apiKey != "" && strings.TrimSpace(r.Header.Get(s.cfg.apiKeyHeader)) != s.cfg.apiKey {
		s.metrics.requestsAuthErr.Add(1)
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusUnauthorized, ingest.Response{Rejected: 1})
		return
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
		if s.cfg.reliabilityMode == "queue" {
			if err := s.ingestSink.WriteEvent(r.Context(), raw, nil); err != nil {
				resp.Rejected++
				s.metrics.eventsRejected.Add(1)
				s.metrics.sinkWriteErrors.Add(1)
				if isDiskFullErr(err) {
					s.diskHealthy.Store(false)
				}
				s.sinkHealthy.Store(false)
				logJSON("error", "collector_kafka_enqueue_failed", map[string]any{"error": err.Error()})
				continue
			}
			s.sinkHealthy.Store(true)
			resp.Accepted++
			s.metrics.eventsAccepted.Add(1)
			continue
		}

		if !validation.IsJSONObject(raw) {
			resp.Invalid++
			s.metrics.eventsInvalid.Add(1)
			continue
		}

		if s.cfg.dedupeEnabled {
			eventID, ok := validation.ExtractStringPath(raw, s.cfg.dedupeKey)
			if ok && s.isDuplicate(eventID) {
				resp.Accepted++
				s.metrics.eventsAccepted.Add(1)
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
			if result.Err != nil {
				resp.Rejected++
				s.metrics.eventsRejected.Add(1)
				s.sinkHealthy.Store(false)
				logJSON("error", "collector_sink_write_failed", map[string]any{"error": result.Err.Error()})
				continue
			}
			s.sinkHealthy.Store(true)
		}

		resp.Accepted++
		s.metrics.eventsAccepted.Add(1)
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

func (s *collectorState) metricsHandler() http.Handler {
	s.metricsInit.Do(func() {
		reg := prometheus.NewRegistry()
		reg.MustRegister(
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.RequestsTotalName,
				Help: "Total ingest requests received.",
			}, func() float64 { return float64(s.metrics.requestsTotal.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.RequestsAuthErrorsName,
				Help: "Total ingest requests rejected by auth.",
			}, func() float64 { return float64(s.metrics.requestsAuthErr.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.RequestsLimitedName,
				Help: "Total ingest requests rejected by rate limiting.",
			}, func() float64 { return float64(s.metrics.requestsLimited.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.EventsAcceptedName,
				Help: "Total events accepted by collector.",
			}, func() float64 { return float64(s.metrics.eventsAccepted.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.EventsInvalidName,
				Help: "Total events rejected as invalid JSON objects.",
			}, func() float64 { return float64(s.metrics.eventsInvalid.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.EventsRejectedName,
				Help: "Total events rejected before sink persistence.",
			}, func() float64 { return float64(s.metrics.eventsRejected.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.EventsDedupedName,
				Help: "Total duplicate events short-circuited by dedupe.",
			}, func() float64 { return float64(s.metrics.eventsDeduped.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: collectormetrics.SinkWriteErrorsName,
				Help: "Total sink write failures.",
			}, func() float64 { return float64(s.metrics.sinkWriteErrors.Load()) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: collectormetrics.SpoolBytesName,
				Help: "Current spool file size in bytes.",
			}, func() float64 { return float64(s.metrics.spoolBytes.Load()) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: collectormetrics.SinkHealthName,
				Help: "Collector sink health state (1=healthy,0=unhealthy).",
			}, func() float64 { return float64(boolMetric(s.effectiveSinkHealthy())) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: collectormetrics.DiskHealthName,
				Help: "Collector disk health state (1=healthy,0=unhealthy).",
			}, func() float64 { return float64(boolMetric(s.effectiveDiskHealthy())) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: collectormetrics.SpoolHealthName,
				Help: "Collector spool health state (1=healthy,0=unhealthy).",
			}, func() float64 { return float64(boolMetric(s.effectiveSpoolHealthy())) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: collectormetrics.ReadyName,
				Help: "Collector readiness state (1=ready,0=not ready).",
			}, func() float64 { return float64(boolMetric(s.isReady())) }),
		)
		s.metricsHTTP = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	})
	return s.metricsHTTP
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func logJSON(level, message string, fields map[string]any) {
	body := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"level":     level,
		"message":   message,
	}
	for k, v := range fields {
		body[k] = v
	}
	b, err := json.Marshal(body)
	if err != nil {
		log.Printf(`{"level":"error","message":"collector_log_encode_failed","error":"%s"}`, err.Error())
		return
	}
	log.Print(string(b))
}

func boolMetric(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *collectorState) isReady() bool {
	return s.ready.Load() && s.effectiveSinkHealthy() && s.effectiveSpoolHealthy() && s.effectiveDiskHealthy()
}

func (s *collectorState) effectiveSinkHealthy() bool {
	if s.sinkHealthy.Load() {
		return true
	}
	return s.metrics.sinkWriteErrors.Load() == 0
}

func (s *collectorState) effectiveSpoolHealthy() bool {
	if s.cfg.reliabilityMode != "spool" {
		return true
	}
	if s.spoolHealthy.Load() {
		return true
	}
	return s.metrics.spoolBytes.Load() == 0
}

func (s *collectorState) effectiveDiskHealthy() bool {
	if s.diskHealthy.Load() {
		return true
	}
	return s.metrics.sinkWriteErrors.Load() == 0
}

func (s *collectorState) ensureProcessor() error {
	if s.cfg.reliabilityMode == "queue" {
		return nil
	}
	s.processorMu.Lock()
	defer s.processorMu.Unlock()
	if s.processor != nil {
		return nil
	}
	if s.rng == nil {
		s.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	processor, err := processing.New(processing.Config{
		DeliveryPolicy:          s.cfg.deliveryPolicy,
		RetryEnabled:            s.cfg.retryEnabled,
		RetryMaxAttempts:        s.cfg.retryMaxAttempts,
		RetryInitialBackoff:     s.cfg.retryInitialBackoff,
		RetryMaxBackoff:         s.cfg.retryMaxBackoff,
		RetryJitter:             s.cfg.retryJitter,
		FallbackEnabled:         s.cfg.fallbackEnabled,
		FallbackOnPrimaryFail:   s.cfg.fallbackOnPrimaryFail,
		FallbackOnSecondaryFail: s.cfg.fallbackOnSecondaryFail,
		FallbackOnPolicyFail:    s.cfg.fallbackOnPolicyFail,
		DLQEnabled:              s.cfg.dlqEnabled,
		DLQPath:                 s.cfg.dlqPath,
		DLQOnPrimaryFail:        s.cfg.dlqOnPrimaryFail,
		DLQOnSecondaryFail:      s.cfg.dlqOnSecondaryFail,
		DLQOnFallbackFail:       s.cfg.dlqOnFallbackFail,
		DLQOnPolicyFail:         s.cfg.dlqOnPolicyFail,
		OnDiskFull: func() {
			s.diskHealthy.Store(false)
		},
		Schema: processing.SchemaConfig{
			Mode:           s.cfg.schemaMode,
			SchemaVersion:  s.cfg.schemaSchemaVersion,
			EventVersion:   s.cfg.schemaEventVersion,
			QuarantinePath: s.cfg.schemaQuarantinePath,
			Registry:       s.convertSchemaRegistry(),
		},
	}, s.ingestSink, s.secondarySinks, s.fallbackSink, s.rng)
	if err != nil {
		return err
	}
	s.processor = processor
	return nil
}

func (s *collectorState) initReliability() error {
	if s.cfg.reliabilityMode != "spool" {
		return nil
	}
	if err := os.MkdirAll(s.cfg.spoolDir, 0o755); err != nil {
		return fmt.Errorf("mkdir spool dir: %w", err)
	}
	spoolPath := filepath.Join(s.cfg.spoolDir, s.cfg.spoolFile)
	f, err := os.OpenFile(spoolPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open spool file: %w", err)
	}
	s.spoolFile = f
	if st, err := f.Stat(); err == nil {
		s.metrics.spoolBytes.Store(st.Size())
		s.spoolHealthy.Store(st.Size() <= s.cfg.maxSpoolBytes)
	}

	s.deliveryQueue = make(chan []byte, s.cfg.deliveryQueueSize)
	s.deliveryStop = make(chan struct{})
	s.deliveryWG.Add(1)
	go s.deliveryWorker()
	_ = s.replaySpool()
	return nil
}

func (s *collectorState) closeReliability() {
	if s.deliveryStop != nil {
		close(s.deliveryStop)
		s.deliveryWG.Wait()
	}
	if s.spoolFile != nil {
		_ = s.spoolFile.Close()
	}
	if s.processor != nil {
		_ = s.processor.Close()
	}
}

func (s *collectorState) appendSpool(raw []byte) error {
	s.spoolMu.Lock()
	defer s.spoolMu.Unlock()
	if s.spoolFile == nil {
		return errors.New("spool file is not initialized")
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
		go func() { s.deliveryQueue <- cp }()
	}
}

func (s *collectorState) deliveryWorker() {
	defer s.deliveryWG.Done()
	for {
		select {
		case <-s.deliveryStop:
			return
		case raw := <-s.deliveryQueue:
			if err := s.ensureProcessor(); err != nil {
				s.metrics.sinkWriteErrors.Add(1)
				s.sinkHealthy.Store(false)
				logJSON("error", "collector_pipeline_not_initialized", map[string]any{"error": err.Error()})
				continue
			}
			result := s.processor.Process(context.Background(), raw)
			if failures := result.Outcome.FailureCount(); failures > 0 {
				s.metrics.sinkWriteErrors.Add(int64(failures))
			}
			if result.Err != nil {
				s.sinkHealthy.Store(false)
			} else {
				s.sinkHealthy.Store(true)
			}
		}
	}
}

func (s *collectorState) replaySpool() error {
	if s.spoolFile == nil {
		return nil
	}
	if _, err := s.spoolFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(s.spoolFile)
	buf := make([]byte, 0, 1024*1024)
	sc.Buffer(buf, math.MaxInt32)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 || !json.Valid(line) {
			continue
		}
		s.enqueueDelivery(line)
	}
	_, _ = s.spoolFile.Seek(0, io.SeekEnd)
	return nil
}

func (s *collectorState) convertSchemaRegistry() []processing.SchemaRegistryEntry {
	if len(s.cfg.schemaRegistry) == 0 {
		return nil
	}
	out := make([]processing.SchemaRegistryEntry, len(s.cfg.schemaRegistry))
	for i, e := range s.cfg.schemaRegistry {
		out[i] = processing.SchemaRegistryEntry{
			SchemaVersion:  e.SchemaVersion,
			EventVersion:   e.EventVersion,
			RequiredFields: e.RequiredFields,
		}
	}
	return out
}

func (s *collectorState) isDuplicate(value string) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	s.dedupeMu.Lock()
	defer s.dedupeMu.Unlock()
	now := time.Now()
	for k, ts := range s.dedupeSeenAt {
		if now.Sub(ts) > s.cfg.dedupeWindow {
			delete(s.dedupeSeenAt, k)
		}
	}
	if ts, ok := s.dedupeSeenAt[value]; ok && now.Sub(ts) <= s.cfg.dedupeWindow {
		return true
	}
	s.dedupeSeenAt[value] = now
	return false
}

func runPeriodicCheckpoint(db *sql.DB, interval time.Duration, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if _, err := db.Exec("CHECKPOINT"); err != nil {
				logJSON("error", "collector_duckdb_periodic_checkpoint_failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

func runPeriodicExport(db *sql.DB, cfg collectorConfig, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.NewTicker(cfg.duckDBExportInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if err := exportDuckDBParquet(db, cfg); err != nil {
				logJSON("error", "collector_duckdb_export_failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

func exportDuckDBParquet(db *sql.DB, cfg collectorConfig) error {
	if err := os.MkdirAll(cfg.duckDBExportPath, 0o755); err != nil {
		return err
	}
	target := storagepath.LocalParquetExportPath(cfg.duckDBExportPath, cfg.duckDBTable, time.Now().UTC())
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	query := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET)", cfg.duckDBTable, strings.ReplaceAll(target, "\\", "/"))
	_, err := db.Exec(query)
	return err
}
