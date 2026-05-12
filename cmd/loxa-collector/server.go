package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	processing "github.com/astraive/loxa-collector/internal/processing"
	storagepath "github.com/astraive/loxa-collector/internal/storage"
	"github.com/astraive/loxa-go"
	"github.com/astraive/loxa-go/sinks/duckdb"
	kafkasink "github.com/astraive/loxa-go/sinks/kafka"
	_ "github.com/marcboeker/go-duckdb"
	"golang.org/x/time/rate"
)

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
		logJSON("info", "kafka_sink_initialized", map[string]any{
			"brokers":     cfg.kafkaBrokers,
			"topic":       cfg.kafkaTopic,
			"reliability": "at-least-once (configure broker/producer for exactly-once)",
		})
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
		colIdent, err := quoteSQLIdent(col)
		if err != nil {
			return err
		}
		if typ, ok := cfg.duckDBColumnTypes[path]; ok {
			columns = append(columns, fmt.Sprintf("%s %s", colIdent, typ))
		} else {
			columns = append(columns, fmt.Sprintf("%s TEXT", colIdent))
		}
	}
	if cfg.duckDBStoreRaw {
		rawIdent, err := quoteSQLIdent(cfg.duckDBRawColumn)
		if err != nil {
			return err
		}
		columns = append(columns, fmt.Sprintf("%s TEXT", rawIdent))
	}
	tableIdent, err := quoteSQLIdent(cfg.duckDBTable)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableIdent, strings.Join(columns, ", "))
	_, execErr := db.Exec(query)
	return execErr
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
	tableIdent, err := quoteSQLIdent(cfg.duckDBTable)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("COPY %s TO %s (FORMAT PARQUET)", tableIdent, quoteSQLString(strings.ReplaceAll(target, "\\", "/")))
	_, execErr := db.Exec(query)
	return execErr
}

func quoteSQLIdent(ident string) (string, error) {
	ident = strings.TrimSpace(ident)
	if ident == "" {
		return "", fmt.Errorf("sql identifier cannot be empty")
	}
	if !configIdentPattern.MatchString(ident) {
		return "", fmt.Errorf("invalid sql identifier %q", ident)
	}
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`, nil
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
