package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	collectorconfig "github.com/astraive/loxa-collector/internal/config"
	"gopkg.in/yaml.v3"
)

var (
	configIdentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	configPathPattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
)

type fileConfig = collectorconfig.Config

func defaultFileConfig() fileConfig { return collectorconfig.Default() }

func loadCollectorConfigFromArgs(args []string) (collectorConfig, error) {
	cfgFile := ""
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.StringVar(&cfgFile, "c", "", "path to config file")
	addr := fs.String("addr", "", "collector listen address")
	duckdbPath := fs.String("duckdb-path", "", "duckdb file path")
	apiKey := fs.String("api-key", "", "collector API key")
	maxBodyBytes := fs.Int64("max-body-bytes", 0, "max request body bytes")
	maxEvents := fs.Int("max-events", 0, "max events per request")
	batchSize := fs.Int("batch-size", 0, "duckdb batch size")
	flushInterval := fs.Duration("flush-interval", 0, "duckdb flush interval")
	if err := fs.Parse(args); err != nil {
		return collectorConfig{}, err
	}
	explicitFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		explicitFlags[f.Name] = true
	})

	fc := defaultFileConfig()
	if cfgFile != "" {
		if err := mergeConfigFile(&fc, cfgFile); err != nil {
			return collectorConfig{}, err
		}
	}
	if err := applyEnvOverrides(&fc); err != nil {
		return collectorConfig{}, err
	}
	applyFlagOverrides(&fc, explicitFlags, *addr, *duckdbPath, *apiKey, *maxBodyBytes, *maxEvents, *batchSize, *flushInterval)

	if err := validateFileConfig(fc); err != nil {
		return collectorConfig{}, err
	}
	return runtimeConfigFromFile(fc), nil
}

func configCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("expected subcommand: print or validate")
	}
	switch args[0] {
	case "print":
		fc, err := loadPrintableConfig(args[1:])
		if err != nil {
			return err
		}
		out, err := marshalPrintableConfig(fc)
		if err != nil {
			return err
		}
		fmt.Print(string(out))
		return nil
	case "validate":
		if _, err := loadPrintableConfig(args[1:]); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown config subcommand %q", args[0])
	}
}

func loadPrintableConfig(args []string) (fileConfig, error) {
	cfgFile := ""
	fs := flag.NewFlagSet("config", flag.ContinueOnError)
	fs.StringVar(&cfgFile, "c", "", "path to config file")
	if err := fs.Parse(args); err != nil {
		return fileConfig{}, err
	}
	fc := defaultFileConfig()
	if cfgFile != "" {
		if err := mergeConfigFile(&fc, cfgFile); err != nil {
			return fileConfig{}, err
		}
	}
	if err := applyEnvOverrides(&fc); err != nil {
		return fileConfig{}, err
	}
	if err := validateFileConfig(fc); err != nil {
		return fileConfig{}, err
	}
	return fc, nil
}

func mergeConfigFile(dst *fileConfig, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extraDoc fileConfig
	if err := dec.Decode(&extraDoc); err != io.EOF {
		if err == nil {
			return errors.New("config file must contain a single YAML document")
		}
		return err
	}
	return nil
}

func applyEnvOverrides(fc *fileConfig) error {
	get := func(key string) (string, bool) {
		v, ok := os.LookupEnv(key)
		if !ok {
			return "", false
		}
		v = strings.TrimSpace(v)
		if v == "" {
			return "", false
		}
		return v, true
	}
	setDuration := func(key string, dst *time.Duration) error {
		v, ok := get(key)
		if !ok {
			return nil
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: invalid duration %q", key, v)
		}
		*dst = d
		return nil
	}
	setInt := func(key string, dst *int) error {
		v, ok := get(key)
		if !ok {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: invalid int %q", key, v)
		}
		*dst = n
		return nil
	}
	setInt64 := func(key string, dst *int64) error {
		v, ok := get(key)
		if !ok {
			return nil
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("%s: invalid int64 %q", key, v)
		}
		*dst = n
		return nil
	}
	setFloat64 := func(key string, dst *float64) error {
		v, ok := get(key)
		if !ok {
			return nil
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("%s: invalid float %q", key, v)
		}
		*dst = f
		return nil
	}
	setBool := func(key string, dst *bool) error {
		v, ok := get(key)
		if !ok {
			return nil
		}
		b, err := parseConfigBool(v)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		*dst = b
		return nil
	}
	setString := func(key string, dst *string) {
		if v, ok := get(key); ok {
			*dst = v
		}
	}
	setStringLower := func(key string, dst *string) {
		if v, ok := get(key); ok {
			*dst = strings.ToLower(v)
		}
	}
	setCSV := func(key string, dst *[]string) {
		v, ok := get(key)
		if !ok {
			return
		}
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		*dst = out
	}

	setString("COLLECTOR_ADDR", &fc.Collector.Addr)
	if err := setDuration("COLLECTOR_READ_HEADER_TIMEOUT", &fc.Collector.ReadHeaderTimeout); err != nil {
		return err
	}
	if err := setDuration("COLLECTOR_SHUTDOWN_TIMEOUT", &fc.Collector.ShutdownTimeout); err != nil {
		return err
	}
	if err := setInt64("COLLECTOR_MAX_BODY_BYTES", &fc.Collector.MaxBodyBytes); err != nil {
		return err
	}
	if err := setInt("COLLECTOR_MAX_EVENTS", &fc.Collector.MaxEventsPerReq); err != nil {
		return err
	}
	setString("COLLECTOR_API_KEY_HEADER", &fc.Auth.Header)
	if v, ok := get("COLLECTOR_API_KEY"); ok {
		fc.Auth.Value = v
		fc.Auth.Enabled = true
	}
	if err := setBool("COLLECTOR_RATE_LIMIT_ENABLED", &fc.RateLimit.Enabled); err != nil {
		return err
	}
	if err := setFloat64("COLLECTOR_RATE_LIMIT_RPS", &fc.RateLimit.RPS); err != nil {
		return err
	}
	if err := setInt("COLLECTOR_RATE_LIMIT_BURST", &fc.RateLimit.Burst); err != nil {
		return err
	}
	setString("DUCKDB_PATH", &fc.DuckDB.Path)
	setString("DUCKDB_DRIVER", &fc.DuckDB.Driver)
	setString("DUCKDB_TABLE", &fc.DuckDB.Table)
	setString("DUCKDB_RAW_COLUMN", &fc.DuckDB.RawColumn)
	setCSV("COLLECTOR_KAFKA_BROKERS", &fc.Kafka.Brokers)
	setString("COLLECTOR_KAFKA_TOPIC", &fc.Kafka.Topic)
	setString("LOXA_WORKER_CONSUMER_GROUP", &fc.Worker.ConsumerGroup)
	if err := setDuration("LOXA_WORKER_POLL_TIMEOUT", &fc.Worker.PollTimeout); err != nil {
		return err
	}
	if err := setBool("DUCKDB_CHECKPOINT_ON_SHUTDOWN", &fc.DuckDB.CheckpointOnShutdown); err != nil {
		return err
	}
	if err := setInt("DUCKDB_BATCH_SIZE", &fc.DuckDB.BatchSize); err != nil {
		return err
	}
	if err := setDuration("DUCKDB_FLUSH_INTERVAL", &fc.DuckDB.FlushInterval); err != nil {
		return err
	}
	if err := setBool("DUCKDB_WRITER_LOOP", &fc.DuckDB.WriterLoop); err != nil {
		return err
	}
	if err := setInt("DUCKDB_WRITER_QUEUE_SIZE", &fc.DuckDB.WriterQueueSize); err != nil {
		return err
	}
	if err := setDuration("DUCKDB_CHECKPOINT_INTERVAL", &fc.DuckDB.CheckpointInterval); err != nil {
		return err
	}
	if err := setBool("DUCKDB_EXPORT_ENABLED", &fc.DuckDB.Export.Enabled); err != nil {
		return err
	}
	setStringLower("DUCKDB_EXPORT_FORMAT", &fc.DuckDB.Export.Format)
	if err := setDuration("DUCKDB_EXPORT_INTERVAL", &fc.DuckDB.Export.Interval); err != nil {
		return err
	}
	setString("DUCKDB_EXPORT_PATH", &fc.DuckDB.Export.Path)
	setStringLower("COLLECTOR_RELIABILITY_MODE", &fc.Reliability.Mode)
	setString("COLLECTOR_SPOOL_DIR", &fc.Reliability.SpoolDir)
	setString("COLLECTOR_SPOOL_FILE", &fc.Reliability.SpoolFile)
	if err := setInt64("COLLECTOR_MAX_SPOOL_BYTES", &fc.Reliability.MaxSpoolBytes); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_SPOOL_FSYNC", &fc.Reliability.Fsync); err != nil {
		return err
	}
	if err := setInt("COLLECTOR_DELIVERY_QUEUE_SIZE", &fc.Reliability.DeliveryQueueSize); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_RETRY_ENABLED", &fc.Retry.Enabled); err != nil {
		return err
	}
	if err := setInt("COLLECTOR_RETRY_MAX_ATTEMPTS", &fc.Retry.MaxAttempts); err != nil {
		return err
	}
	if err := setDuration("COLLECTOR_RETRY_INITIAL_BACKOFF", &fc.Retry.InitialBackoff); err != nil {
		return err
	}
	if err := setDuration("COLLECTOR_RETRY_MAX_BACKOFF", &fc.Retry.MaxBackoff); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_RETRY_JITTER", &fc.Retry.Jitter); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_DLQ_ENABLED", &fc.DeadLetter.Enabled); err != nil {
		return err
	}
	setString("COLLECTOR_DLQ_PATH", &fc.DeadLetter.Path)
	setStringLower("COLLECTOR_DELIVERY_POLICY", &fc.Fanout.Delivery.Policy)
	if err := setBool("COLLECTOR_FANOUT_FALLBACK_ENABLED", &fc.Fanout.Delivery.Fallback.Enabled); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_FANOUT_FALLBACK_ON_PRIMARY_FAILURE", &fc.Fanout.Delivery.Fallback.OnPrimaryFailure); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_FANOUT_FALLBACK_ON_SECONDARY_FAILURE", &fc.Fanout.Delivery.Fallback.OnSecondaryFailure); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_FANOUT_FALLBACK_ON_POLICY_FAILURE", &fc.Fanout.Delivery.Fallback.OnPolicyFailure); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_FANOUT_DLQ_ON_PRIMARY_FAILURE", &fc.Fanout.Delivery.DLQ.OnPrimaryFailure); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_FANOUT_DLQ_ON_SECONDARY_FAILURE", &fc.Fanout.Delivery.DLQ.OnSecondaryFailure); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_FANOUT_DLQ_ON_FALLBACK_FAILURE", &fc.Fanout.Delivery.DLQ.OnFallbackFailure); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_FANOUT_DLQ_ON_POLICY_FAILURE", &fc.Fanout.Delivery.DLQ.OnPolicyFailure); err != nil {
		return err
	}
	if err := setBool("COLLECTOR_DEDUPE_ENABLED", &fc.Dedupe.Enabled); err != nil {
		return err
	}
	setString("COLLECTOR_DEDUPE_KEY", &fc.Dedupe.Key)
	if err := setDuration("COLLECTOR_DEDUPE_WINDOW", &fc.Dedupe.Window); err != nil {
		return err
	}
	setStringLower("COLLECTOR_DEDUPE_BACKEND", &fc.Dedupe.Backend)

	if fc.Auth.Value == "" && fc.Auth.ValueEnv != "" {
		fc.Auth.Value = strings.TrimSpace(os.Getenv(fc.Auth.ValueEnv))
		if fc.Auth.Value != "" {
			fc.Auth.Enabled = true
		}
	}
	return nil
}

func applyFlagOverrides(fc *fileConfig, explicit map[string]bool, addr, duckdbPath, apiKey string, maxBodyBytes int64, maxEvents, batchSize int, flushInterval time.Duration) {
	if explicit["addr"] {
		fc.Collector.Addr = addr
	}
	if explicit["duckdb-path"] {
		fc.DuckDB.Path = duckdbPath
	}
	if explicit["api-key"] {
		fc.Auth.Value = apiKey
		fc.Auth.Enabled = true
	}
	if explicit["max-body-bytes"] {
		fc.Collector.MaxBodyBytes = maxBodyBytes
	}
	if explicit["max-events"] {
		fc.Collector.MaxEventsPerReq = maxEvents
	}
	if explicit["batch-size"] {
		fc.DuckDB.BatchSize = batchSize
	}
	if explicit["flush-interval"] {
		fc.DuckDB.FlushInterval = flushInterval
	}
}

func validateFileConfig(fc fileConfig) error {
	if strings.TrimSpace(fc.Collector.Addr) == "" {
		return errors.New("collector.addr must not be empty")
	}
	if fc.Collector.ReadHeaderTimeout <= 0 {
		return errors.New("collector.read_header_timeout must be > 0")
	}
	if fc.Collector.ShutdownTimeout <= 0 {
		return errors.New("collector.shutdown_timeout must be > 0")
	}
	if fc.Collector.MaxBodyBytes <= 0 {
		return errors.New("collector.max_body_bytes must be > 0")
	}
	if fc.Collector.MaxEventsPerReq <= 0 {
		return errors.New("collector.max_events_per_request must be > 0")
	}
	if fc.Auth.Enabled && strings.TrimSpace(fc.Auth.Value) == "" {
		return errors.New("auth enabled but no API key resolved")
	}
	if strings.TrimSpace(fc.Auth.Header) == "" {
		return errors.New("auth.header must not be empty")
	}
	if fc.RateLimit.Enabled {
		if fc.RateLimit.RPS <= 0 {
			return errors.New("rate_limit.rps must be > 0")
		}
		if fc.RateLimit.Burst <= 0 {
			return errors.New("rate_limit.burst must be > 0")
		}
	}
	if strings.TrimSpace(fc.Storage.Primary) != "duckdb" {
		return errors.New("storage.primary must currently be duckdb")
	}
	mode := strings.ToLower(strings.TrimSpace(fc.Reliability.Mode))
	if mode != "direct" && mode != "spool" && mode != "queue" {
		return errors.New("reliability.mode must be direct, spool, or queue")
	}
	if mode == "spool" {
		if strings.TrimSpace(fc.Reliability.SpoolDir) == "" {
			return errors.New("reliability.spool_dir must not be empty in spool mode")
		}
		if strings.TrimSpace(fc.Reliability.SpoolFile) == "" {
			return errors.New("reliability.spool_file must not be empty in spool mode")
		}
		if fc.Reliability.MaxSpoolBytes <= 0 {
			return errors.New("reliability.max_spool_bytes must be > 0 in spool mode")
		}
		if fc.Reliability.DeliveryQueueSize <= 0 {
			return errors.New("reliability.delivery_queue_size must be > 0 in spool mode")
		}
	}
	if mode == "queue" {
		if len(fc.Kafka.Brokers) == 0 {
			return errors.New("kafka.brokers must include at least one broker in queue mode")
		}
		for i, broker := range fc.Kafka.Brokers {
			if strings.TrimSpace(broker) == "" {
				return fmt.Errorf("kafka.brokers[%d] must not be empty", i)
			}
		}
		if strings.TrimSpace(fc.Kafka.Topic) == "" {
			return errors.New("kafka.topic must not be empty in queue mode")
		}
	}
	if strings.TrimSpace(fc.Worker.ConsumerGroup) == "" {
		return errors.New("worker.consumer_group must not be empty")
	}
	if fc.Worker.PollTimeout <= 0 {
		return errors.New("worker.poll_timeout must be > 0")
	}
	if fc.Retry.Enabled {
		if fc.Retry.MaxAttempts <= 0 {
			return errors.New("retry.max_attempts must be > 0")
		}
		if fc.Retry.InitialBackoff <= 0 || fc.Retry.MaxBackoff <= 0 {
			return errors.New("retry backoff durations must be > 0")
		}
	}
	if fc.DeadLetter.Enabled && strings.TrimSpace(fc.DeadLetter.Path) == "" {
		return errors.New("dead_letter.path must not be empty when dead_letter.enabled is true")
	}
	policy := strings.ToLower(strings.TrimSpace(fc.Fanout.Delivery.Policy))
	switch policy {
	case deliveryPolicyBestEffort, deliveryPolicyRequirePrimary, deliveryPolicyRequireAll:
	default:
		return errors.New("fanout.delivery.policy must be best_effort, require_primary, or require_all")
	}
	seenFanoutNames := map[string]struct{}{}
	enabledFallbackOutputs := 0
	for i, output := range fc.Fanout.Outputs {
		name := strings.TrimSpace(output.Name)
		if name == "" {
			return fmt.Errorf("fanout.outputs[%d].name must not be empty", i)
		}
		if _, ok := seenFanoutNames[name]; ok {
			return fmt.Errorf("fanout.outputs[%d].name %q must be unique", i, name)
		}
		seenFanoutNames[name] = struct{}{}

		role := strings.ToLower(strings.TrimSpace(output.Role))
		if role == "" {
			role = fanoutRoleSecondary
		}
		if role != fanoutRoleSecondary && role != fanoutRoleFallback {
			return fmt.Errorf("fanout.outputs[%d].role must be %q or %q", i, fanoutRoleSecondary, fanoutRoleFallback)
		}
		sinkType := strings.ToLower(strings.TrimSpace(output.Type))
		if sinkType == "" {
			sinkType = fanoutSinkDuckDB
		}
		if sinkType != fanoutSinkDuckDB && sinkType != fanoutSinkLoki && sinkType != fanoutSinkOTLP &&
			sinkType != fanoutSinkClickHouse && sinkType != fanoutSinkS3 && sinkType != fanoutSinkGCS {
			return fmt.Errorf("fanout.outputs[%d].type %q is not supported", i, sinkType)
		}

		if !output.Enabled {
			continue
		}
		switch sinkType {
		case fanoutSinkDuckDB:
			if strings.TrimSpace(output.DuckDB.Path) == "" {
				return fmt.Errorf("fanout.outputs[%d].duckdb.path must not be empty when enabled", i)
			}
			if table := strings.TrimSpace(output.DuckDB.Table); table != "" && !validTableName(table) {
				return fmt.Errorf("fanout.outputs[%d].duckdb.table %q is invalid", i, table)
			}
			if rawColumn := strings.TrimSpace(output.DuckDB.RawColumn); rawColumn != "" && !configIdentPattern.MatchString(rawColumn) {
				return fmt.Errorf("fanout.outputs[%d].duckdb.raw_column %q is invalid", i, rawColumn)
			}
		case fanoutSinkLoki:
			if strings.TrimSpace(output.Loki.URL) == "" {
				return fmt.Errorf("fanout.outputs[%d].loki.url must not be empty when enabled", i)
			}
		case fanoutSinkOTLP:
			if strings.TrimSpace(output.OTLP.Endpoint) == "" {
				return fmt.Errorf("fanout.outputs[%d].otlp.endpoint must not be empty when enabled", i)
			}
		case fanoutSinkClickHouse:
			if len(output.ClickHouse.Addrs) == 0 {
				return fmt.Errorf("fanout.outputs[%d].clickhouse.addrs must not be empty when enabled", i)
			}
		case fanoutSinkS3:
			if strings.TrimSpace(output.S3.Bucket) == "" {
				return fmt.Errorf("fanout.outputs[%d].s3.bucket must not be empty when enabled", i)
			}
		case fanoutSinkGCS:
			if strings.TrimSpace(output.GCS.Bucket) == "" {
				return fmt.Errorf("fanout.outputs[%d].gcs.bucket must not be empty when enabled", i)
			}
		}
		if role == fanoutRoleFallback {
			enabledFallbackOutputs++
		}
	}
	if enabledFallbackOutputs > 1 {
		return errors.New("fanout supports at most one enabled fallback output")
	}
	if fc.Fanout.Delivery.Fallback.Enabled && enabledFallbackOutputs != 1 {
		return errors.New("fanout.delivery.fallback.enabled requires exactly one enabled fallback output")
	}
	if fc.Dedupe.Enabled {
		if strings.TrimSpace(fc.Dedupe.Key) == "" {
			return errors.New("dedupe.key must not be empty when dedupe.enabled is true")
		}
		if fc.Dedupe.Window <= 0 {
			return errors.New("dedupe.window must be > 0 when dedupe.enabled is true")
		}
		if strings.ToLower(strings.TrimSpace(fc.Dedupe.Backend)) != "memory" {
			return errors.New("dedupe.backend must currently be memory")
		}
	}
	if !configIdentPattern.MatchString(fc.DuckDB.RawColumn) {
		return fmt.Errorf("invalid duckdb.raw_column %q", fc.DuckDB.RawColumn)
	}
	if strings.TrimSpace(fc.DuckDB.Path) == "" {
		return errors.New("duckdb.path must not be empty")
	}
	if strings.TrimSpace(fc.DuckDB.Driver) == "" {
		return errors.New("duckdb.driver must not be empty")
	}
	if !validTableName(fc.DuckDB.Table) {
		return fmt.Errorf("invalid duckdb.table %q", fc.DuckDB.Table)
	}
	if fc.DuckDB.BatchSize < 0 {
		return errors.New("duckdb.batch_size must be >= 0")
	}
	if fc.DuckDB.WriterQueueSize < 0 {
		return errors.New("duckdb.writer_queue_size must be >= 0")
	}
	if fc.DuckDB.MaxOpenConns <= 0 || fc.DuckDB.MaxIdleConns <= 0 {
		return errors.New("duckdb max_open_conns and max_idle_conns must be > 0")
	}
	if fc.DuckDB.CheckpointInterval < 0 {
		return errors.New("duckdb.checkpoint_interval must be >= 0")
	}
	if fc.DuckDB.FlushInterval < 0 {
		return errors.New("duckdb.flush_interval must be >= 0")
	}
	if fc.DuckDB.Export.Enabled {
		if strings.ToLower(strings.TrimSpace(fc.DuckDB.Export.Format)) != "parquet" {
			return errors.New("duckdb.export.format must currently be parquet")
		}
		if fc.DuckDB.Export.Interval <= 0 {
			return errors.New("duckdb.export.interval must be > 0 when export is enabled")
		}
		if strings.TrimSpace(fc.DuckDB.Export.Path) == "" {
			return errors.New("duckdb.export.path must not be empty when export is enabled")
		}
	}
	for col, path := range fc.DuckDB.Schema {
		if !configIdentPattern.MatchString(col) {
			return fmt.Errorf("invalid duckdb schema column %q", col)
		}
		if !configPathPattern.MatchString(path) {
			return fmt.Errorf("invalid duckdb schema path %q", path)
		}
	}
	for path, typ := range fc.DuckDB.ColumnTypes {
		if !configPathPattern.MatchString(path) {
			return fmt.Errorf("invalid duckdb column_types path %q", path)
		}
		if strings.TrimSpace(typ) == "" {
			return fmt.Errorf("duckdb column_types type for path %q must not be empty", path)
		}
	}
	for _, route := range []string{fc.Routes.Ingest, fc.Routes.Health, fc.Routes.Ready, fc.Routes.Metrics} {
		if !strings.HasPrefix(route, "/") {
			return fmt.Errorf("route %q must start with /", route)
		}
	}
	return nil
}

func runtimeConfigFromFile(fc fileConfig) collectorConfig {
	return collectorConfig{
		addr:                    fc.Collector.Addr,
		readHeaderTimeout:       fc.Collector.ReadHeaderTimeout,
		shutdownTimeout:         fc.Collector.ShutdownTimeout,
		maxBodyBytes:            fc.Collector.MaxBodyBytes,
		maxEventsPerRequest:     fc.Collector.MaxEventsPerReq,
		authEnabled:             fc.Auth.Enabled,
		apiKeyHeader:            fc.Auth.Header,
		apiKey:                  fc.Auth.Value,
		rateLimitEnabled:        fc.RateLimit.Enabled,
		rateLimitRPS:            fc.RateLimit.RPS,
		rateLimitBurst:          fc.RateLimit.Burst,
		ingestPath:              fc.Routes.Ingest,
		healthPath:              fc.Routes.Health,
		readyPath:               fc.Routes.Ready,
		metricsPath:             fc.Routes.Metrics,
		storagePrimary:          fc.Storage.Primary,
		duckDBPath:              fc.DuckDB.Path,
		duckDBDriver:            fc.DuckDB.Driver,
		duckDBTable:             fc.DuckDB.Table,
		duckDBRawColumn:         fc.DuckDB.RawColumn,
		duckDBStoreRaw:          fc.DuckDB.StoreRaw,
		duckDBCheckpointOnStop:  fc.DuckDB.CheckpointOnShutdown,
		duckDBMaxOpenConns:      fc.DuckDB.MaxOpenConns,
		duckDBMaxIdleConns:      fc.DuckDB.MaxIdleConns,
		duckDBBatchSize:         fc.DuckDB.BatchSize,
		duckDBFlushInterval:     fc.DuckDB.FlushInterval,
		duckDBWriterLoop:        fc.DuckDB.WriterLoop,
		duckDBWriterQueueSize:   fc.DuckDB.WriterQueueSize,
		duckDBCheckpointIntvl:   fc.DuckDB.CheckpointInterval,
		duckDBExportEnabled:     fc.DuckDB.Export.Enabled,
		duckDBExportFormat:      strings.ToLower(fc.DuckDB.Export.Format),
		duckDBExportInterval:    fc.DuckDB.Export.Interval,
		duckDBExportPath:        fc.DuckDB.Export.Path,
		duckDBSchema:            fc.DuckDB.Schema,
		duckDBColumnTypes:       fc.DuckDB.ColumnTypes,
		loggingLevel:            fc.Logging.Level,
		loggingFormat:           fc.Logging.Format,
		metricsPrometheus:       fc.Metrics.Prometheus,
		reliabilityMode:         strings.ToLower(fc.Reliability.Mode),
		spoolDir:                fc.Reliability.SpoolDir,
		spoolFile:               fc.Reliability.SpoolFile,
		maxSpoolBytes:           fc.Reliability.MaxSpoolBytes,
		spoolFsync:              fc.Reliability.Fsync,
		deliveryQueueSize:       fc.Reliability.DeliveryQueueSize,
		retryEnabled:            fc.Retry.Enabled,
		retryMaxAttempts:        fc.Retry.MaxAttempts,
		retryInitialBackoff:     fc.Retry.InitialBackoff,
		retryMaxBackoff:         fc.Retry.MaxBackoff,
		retryJitter:             fc.Retry.Jitter,
		dlqEnabled:              fc.DeadLetter.Enabled,
		dlqPath:                 fc.DeadLetter.Path,
		fanoutOutputs:           fanoutOutputsFromFile(fc.Fanout.Outputs),
		deliveryPolicy:          strings.ToLower(fc.Fanout.Delivery.Policy),
		fallbackEnabled:         fc.Fanout.Delivery.Fallback.Enabled,
		fallbackOnPrimaryFail:   fc.Fanout.Delivery.Fallback.OnPrimaryFailure,
		fallbackOnSecondaryFail: fc.Fanout.Delivery.Fallback.OnSecondaryFailure,
		fallbackOnPolicyFail:    fc.Fanout.Delivery.Fallback.OnPolicyFailure,
		dlqOnPrimaryFail:        fc.Fanout.Delivery.DLQ.OnPrimaryFailure,
		dlqOnSecondaryFail:      fc.Fanout.Delivery.DLQ.OnSecondaryFailure,
		dlqOnFallbackFail:       fc.Fanout.Delivery.DLQ.OnFallbackFailure,
		dlqOnPolicyFail:         fc.Fanout.Delivery.DLQ.OnPolicyFailure,
		dedupeEnabled:           fc.Dedupe.Enabled,
		dedupeKey:               fc.Dedupe.Key,
		dedupeWindow:            fc.Dedupe.Window,
		dedupeBackend:           strings.ToLower(fc.Dedupe.Backend),
		kafkaBrokers:            append([]string(nil), fc.Kafka.Brokers...),
		kafkaTopic:              strings.TrimSpace(fc.Kafka.Topic),
		workerConsumerGroup:     strings.TrimSpace(fc.Worker.ConsumerGroup),
		workerPollTimeout:       fc.Worker.PollTimeout,
		schemaMode:              strings.ToLower(strings.TrimSpace(fc.Schema.Mode)),
		schemaSchemaVersion:     strings.TrimSpace(fc.Schema.SchemaVersion),
		schemaEventVersion:      strings.TrimSpace(fc.Schema.EventVersion),
		schemaQuarantinePath:    strings.TrimSpace(fc.Schema.QuarantinePath),
		schemaRegistry:          fc.Schema.Registry,
	}
}

func fanoutOutputsFromFile(outputs []collectorconfig.FanoutOutputConfig) []collectorFanoutOutput {
	mapped := make([]collectorFanoutOutput, 0, len(outputs))
	for _, output := range outputs {
		mapped = append(mapped, collectorFanoutOutput{
			name:            strings.TrimSpace(output.Name),
			role:            strings.ToLower(strings.TrimSpace(output.Role)),
			sinkType:        strings.ToLower(strings.TrimSpace(output.Type)),
			enabled:         output.Enabled,
			duckDBPath:      strings.TrimSpace(output.DuckDB.Path),
			duckDBTable:     strings.TrimSpace(output.DuckDB.Table),
			duckDBRawColumn: strings.TrimSpace(output.DuckDB.RawColumn),
			duckDBStoreRaw:  output.DuckDB.StoreRaw,
			lokiURL:         strings.TrimSpace(output.Loki.URL),
			lokiTenantID:    strings.TrimSpace(output.Loki.TenantID),
			lokiLabels:      output.Loki.Labels,
			lokiBatchSize:   output.Loki.BatchSize,
			lokiFlushIntvl:  output.Loki.FlushInterval,
			lokiTimeout:     output.Loki.Timeout,
			otlpEndpoint:    strings.TrimSpace(output.OTLP.Endpoint),
			otlpInsecure:    output.OTLP.Insecure,
			otlpProtocol:    strings.ToLower(strings.TrimSpace(output.OTLP.Protocol)),
			otlpTimeout:     output.OTLP.Timeout,
			otlpLogger:      strings.TrimSpace(output.OTLP.Logger),
			chAddrs:         output.ClickHouse.Addrs,
			chDatabase:      strings.TrimSpace(output.ClickHouse.Database),
			chUsername:      strings.TrimSpace(output.ClickHouse.Username),
			chPassword:      output.ClickHouse.Password,
			chTable:         strings.TrimSpace(output.ClickHouse.Table),
			chSchema:        output.ClickHouse.Schema,
			chStoreRaw:      output.ClickHouse.StoreRaw,
			chRawColumn:     strings.TrimSpace(output.ClickHouse.RawColumn),
			s3Bucket:        strings.TrimSpace(output.S3.Bucket),
			s3Prefix:        strings.TrimSpace(output.S3.Prefix),
			s3Region:        strings.TrimSpace(output.S3.Region),
			s3Endpoint:      strings.TrimSpace(output.S3.Endpoint),
			s3AccessKey:     strings.TrimSpace(output.S3.AccessKey),
			s3SecretKey:     output.S3.SecretKey,
			s3Schema:        output.S3.Schema,
			s3BatchSize:     output.S3.BatchSize,
			s3FlushIntvl:    output.S3.FlushInterval,
			gcsBucket:       strings.TrimSpace(output.GCS.Bucket),
			gcsPrefix:       strings.TrimSpace(output.GCS.Prefix),
			gcsCredsFile:    strings.TrimSpace(output.GCS.CredentialsFile),
			gcsSchema:       output.GCS.Schema,
			gcsBatchSize:    output.GCS.BatchSize,
			gcsFlushIntvl:   output.GCS.FlushInterval,
		})
	}
	return mapped
}

func validTableName(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, part := range strings.Split(name, ".") {
		if !configIdentPattern.MatchString(part) {
			return false
		}
	}
	return true
}

func parseConfigBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool %q", raw)
	}
}

func marshalPrintableConfig(fc fileConfig) ([]byte, error) {
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(fc); err != nil {
		_ = enc.Close()
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
