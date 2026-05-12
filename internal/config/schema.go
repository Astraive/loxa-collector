package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Collector   CollectorConfig        `yaml:"collector"`
	Auth        AuthConfig             `yaml:"auth"`
	RateLimit   RateLimitConfig        `yaml:"rate_limit"`
	Routes      RoutesConfig           `yaml:"routes"`
	Storage     StorageConfig          `yaml:"storage"`
	DuckDB      DuckDBConfig           `yaml:"duckdb"`
	Kafka       KafkaConfig            `yaml:"kafka"`
	Worker      WorkerConfig           `yaml:"worker"`
	Logging     LoggingConfig          `yaml:"logging"`
	Metrics     MetricsConfig          `yaml:"metrics"`
	Reliability ReliabilityConfig      `yaml:"reliability"`
	Retry       RetryConfig            `yaml:"retry"`
	DeadLetter  DeadLetterConfig       `yaml:"dead_letter"`
	Fanout      FanoutConfig           `yaml:"fanout"`
	Dedupe      DedupeConfig           `yaml:"dedupe"`
	Schema      SchemaGovernanceConfig `yaml:"schema_governance"`
}

type CollectorConfig struct {
	Addr              string        `yaml:"addr"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
	MaxBodyBytes      int64         `yaml:"max_body_bytes"`
	MaxEventsPerReq   int           `yaml:"max_events_per_request"`
}

type AuthConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Header   string `yaml:"header"`
	ValueEnv string `yaml:"value_env"`
	Value    string `yaml:"-"`
}

type RateLimitConfig struct {
	Enabled bool    `yaml:"enabled"`
	RPS     float64 `yaml:"rps"`
	Burst   int     `yaml:"burst"`
}

type RoutesConfig struct {
	Ingest  string `yaml:"ingest"`
	Health  string `yaml:"health"`
	Ready   string `yaml:"ready"`
	Metrics string `yaml:"metrics"`
}

type StorageConfig struct {
	Primary string `yaml:"primary"`
}

type DuckDBConfig struct {
	Path                 string            `yaml:"path"`
	Driver               string            `yaml:"driver"`
	Table                string            `yaml:"table"`
	RawColumn            string            `yaml:"raw_column"`
	StoreRaw             bool              `yaml:"store_raw"`
	CheckpointOnShutdown bool              `yaml:"checkpoint_on_shutdown"`
	CheckpointInterval   time.Duration     `yaml:"checkpoint_interval"`
	MaxOpenConns         int               `yaml:"max_open_conns"`
	MaxIdleConns         int               `yaml:"max_idle_conns"`
	BatchSize            int               `yaml:"batch_size"`
	FlushInterval        time.Duration     `yaml:"flush_interval"`
	WriterLoop           bool              `yaml:"writer_loop"`
	WriterQueueSize      int               `yaml:"writer_queue_size"`
	Export               DuckDBExport      `yaml:"export"`
	Schema               map[string]string `yaml:"schema"`
	ColumnTypes          map[string]string `yaml:"column_types"`
}

type DuckDBExport struct {
	Enabled  bool          `yaml:"enabled"`
	Format   string        `yaml:"format"`
	Interval time.Duration `yaml:"interval"`
	Path     string        `yaml:"path"`
}

type KafkaConfig struct {
	Brokers            []string      `yaml:"brokers"`
	Topic              string        `yaml:"topic"`
	Acks               string        `yaml:"acks"`                // 0, 1, all (default: all)
	RequestTimeout     time.Duration `yaml:"request_timeout"`    // per-request timeout
	EnableIdempotence  bool          `yaml:"enable_idempotence"` // exactly-once semantics
	MaxRetries         int           `yaml:"max_retries"`         // producer retry count
	RetryBackoff       time.Duration `yaml:"retry_backoff"`      // backoff between retries
}

type WorkerConfig struct {
	ConsumerGroup string        `yaml:"consumer_group"`
	PollTimeout   time.Duration `yaml:"poll_timeout"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type MetricsConfig struct {
	Prometheus bool `yaml:"prometheus"`
}

type ReliabilityConfig struct {
	Mode              string        `yaml:"mode"`
	SpoolDir          string        `yaml:"spool_dir"`
	SpoolFile         string        `yaml:"spool_file"`
	MaxSpoolBytes     int64         `yaml:"max_spool_bytes"`
	Fsync             bool          `yaml:"fsync"`
	DeliveryQueueSize int           `yaml:"delivery_queue_size"`
	QueueDir          string        `yaml:"queue_dir"`           // local queue directory
	QueueBatchSize    int           `yaml:"queue_batch_size"`   // events per batch
	QueueBatchTimeout time.Duration  `yaml:"queue_batch_timeout"` // batch timeout
	QueueFlushInterval time.Duration `yaml:"queue_flush_interval"` // flush interval
	QueueCircuitThreshold int      `yaml:"queue_circuit_threshold"` // failures before circuit opens
	QueueCircuitTimeout time.Duration `yaml:"queue_circuit_timeout"` // circuit reset timeout
}

type RetryConfig struct {
	Enabled        bool          `yaml:"enabled"`
	MaxAttempts    int           `yaml:"max_attempts"`
	InitialBackoff time.Duration `yaml:"initial_backoff"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	Jitter         bool          `yaml:"jitter"`
}

type DeadLetterConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

type FanoutConfig struct {
	Outputs  []FanoutOutputConfig       `yaml:"outputs"`
	Delivery FanoutDeliveryPolicyConfig `yaml:"delivery"`
}

type FanoutOutputConfig struct {
	Name       string                 `yaml:"name"`
	Role       string                 `yaml:"role"`
	Type       string                 `yaml:"type"`
	Enabled    bool                   `yaml:"enabled"`
	DuckDB     FanoutDuckDBConfig     `yaml:"duckdb"`
	Loki       FanoutLokiConfig       `yaml:"loki"`
	OTLP       FanoutOTLPConfig       `yaml:"otlp"`
	ClickHouse FanoutClickHouseConfig `yaml:"clickhouse"`
	S3         FanoutS3Config         `yaml:"s3"`
	GCS        FanoutGCSConfig        `yaml:"gcs"`
}

type FanoutDuckDBConfig struct {
	Path      string `yaml:"path"`
	Table     string `yaml:"table"`
	RawColumn string `yaml:"raw_column"`
	StoreRaw  *bool  `yaml:"store_raw"`
}

type FanoutLokiConfig struct {
	URL           string            `yaml:"url"`
	TenantID      string            `yaml:"tenant_id"`
	Labels        map[string]string `yaml:"labels"`
	BatchSize     int               `yaml:"batch_size"`
	FlushInterval time.Duration     `yaml:"flush_interval"`
	Timeout       time.Duration     `yaml:"timeout"`
}

type FanoutOTLPConfig struct {
	Endpoint string        `yaml:"endpoint"`
	Insecure bool          `yaml:"insecure"`
	Protocol string        `yaml:"protocol"`
	Timeout  time.Duration `yaml:"timeout"`
	Logger   string        `yaml:"logger"`
}

type FanoutClickHouseConfig struct {
	Addrs     []string          `yaml:"addrs"`
	Database  string            `yaml:"database"`
	Username  string            `yaml:"username"`
	Password  string            `yaml:"password"`
	Table     string            `yaml:"table"`
	Schema    map[string]string `yaml:"schema"`
	StoreRaw  bool              `yaml:"store_raw"`
	RawColumn string            `yaml:"raw_column"`
}

type FanoutS3Config struct {
	Bucket        string            `yaml:"bucket"`
	Prefix        string            `yaml:"prefix"`
	Region        string            `yaml:"region"`
	Endpoint      string            `yaml:"endpoint"`
	AccessKey     string            `yaml:"access_key"`
	SecretKey     string            `yaml:"secret_key"`
	Schema        map[string]string `yaml:"schema"`
	BatchSize     int               `yaml:"batch_size"`
	FlushInterval time.Duration     `yaml:"flush_interval"`
}

type FanoutGCSConfig struct {
	Bucket          string            `yaml:"bucket"`
	Prefix          string            `yaml:"prefix"`
	CredentialsFile string            `yaml:"credentials_file"`
	Schema          map[string]string `yaml:"schema"`
	BatchSize       int               `yaml:"batch_size"`
	FlushInterval   time.Duration     `yaml:"flush_interval"`
}

type FanoutDeliveryPolicyConfig struct {
	Policy   string               `yaml:"policy"`
	Fallback FanoutFallbackConfig `yaml:"fallback"`
	DLQ      FanoutDLQConfig      `yaml:"dlq"`
}

type FanoutFallbackConfig struct {
	Enabled            bool `yaml:"enabled"`
	OnPrimaryFailure   bool `yaml:"on_primary_failure"`
	OnSecondaryFailure bool `yaml:"on_secondary_failure"`
	OnPolicyFailure    bool `yaml:"on_policy_failure"`
}

type FanoutDLQConfig struct {
	OnPrimaryFailure   bool `yaml:"on_primary_failure"`
	OnSecondaryFailure bool `yaml:"on_secondary_failure"`
	OnFallbackFailure  bool `yaml:"on_fallback_failure"`
	OnPolicyFailure    bool `yaml:"on_policy_failure"`
}

type DedupeConfig struct {
	Enabled bool          `yaml:"enabled"`
	Key     string        `yaml:"key"`
	Window  time.Duration `yaml:"window"`
	Backend string        `yaml:"backend"`
}

type SchemaGovernanceConfig struct {
	Mode           string                `yaml:"mode"`
	SchemaVersion  string                `yaml:"schema_version"`
	EventVersion   string                `yaml:"event_version"`
	RegistryFile   string                `yaml:"registry_file"`
	Registry       []SchemaRegistryEntry `yaml:"registry"`
	QuarantinePath string                `yaml:"quarantine_path"`
}

type SchemaRegistryEntry struct {
	SchemaVersion  string   `yaml:"schema_version"`
	EventVersion   string   `yaml:"event_version"`
	RequiredFields []string `yaml:"required_fields"`
}

func Default() Config {
	return Config{
		Collector: CollectorConfig{
			Addr:              ":9090",
			ReadHeaderTimeout: 5 * time.Second,
			ShutdownTimeout:   10 * time.Second,
			MaxBodyBytes:      10 * 1024 * 1024,
			MaxEventsPerReq:   5000,
		},
		Auth: AuthConfig{
			Enabled:  false,
			Header:   "X-API-Key",
			ValueEnv: "COLLECTOR_API_KEY",
		},
		RateLimit: RateLimitConfig{
			Enabled: true,
			RPS:     1000,
			Burst:   1000,
		},
		Routes: RoutesConfig{
			Ingest:  "/ingest",
			Health:  "/healthz",
			Ready:   "/readyz",
			Metrics: "/metrics",
		},
		Storage: StorageConfig{
			Primary: "duckdb",
		},
		DuckDB: DuckDBConfig{
			Path:                 "loxa.db",
			Driver:               "duckdb",
			Table:                "events",
			RawColumn:            "raw",
			StoreRaw:             true,
			CheckpointOnShutdown: true,
			CheckpointInterval:   0,
			MaxOpenConns:         1,
			MaxIdleConns:         1,
			BatchSize:            0,
			FlushInterval:        0,
			WriterLoop:           false,
			WriterQueueSize:      0,
			Export: DuckDBExport{
				Enabled:  false,
				Format:   "parquet",
				Interval: time.Hour,
				Path:     "exports",
			},
			Schema: map[string]string{
				"event_id":    "event_id",
				"event_type":  "event",
				"service":     "service",
				"status_code": "http.status",
				"duration_ms": "duration_ms",
				"timestamp":   "timestamp",
			},
			ColumnTypes: map[string]string{
				"status_code": "INTEGER",
				"http.status": "INTEGER",
				"duration_ms": "DOUBLE",
				"timestamp":   "TIMESTAMP",
			},
		},
		Kafka: KafkaConfig{
			Brokers: []string{"127.0.0.1:9092"},
			Topic:   "loxa-events",
		},
		Worker: WorkerConfig{
			ConsumerGroup: "loxa-worker",
			PollTimeout:   2 * time.Second,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
		Metrics: MetricsConfig{
			Prometheus: true,
		},
		Reliability: ReliabilityConfig{
			Mode:              "direct",
			SpoolDir:          "loxa-spool",
			SpoolFile:         "spool.ndjson",
			MaxSpoolBytes:     10 * 1024 * 1024 * 1024,
			Fsync:             true,
			DeliveryQueueSize: 4096,
		},
		Retry: RetryConfig{
			Enabled:        true,
			MaxAttempts:    10,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     30 * time.Second,
			Jitter:         true,
		},
		DeadLetter: DeadLetterConfig{
			Enabled: false,
			Path:    "loxa-dlq.ndjson",
		},
		Fanout: FanoutConfig{
			Outputs: nil,
			Delivery: FanoutDeliveryPolicyConfig{
				Policy: "require_primary",
				Fallback: FanoutFallbackConfig{
					Enabled:            false,
					OnPrimaryFailure:   false,
					OnSecondaryFailure: false,
					OnPolicyFailure:    true,
				},
				DLQ: FanoutDLQConfig{
					OnPrimaryFailure:   true,
					OnSecondaryFailure: false,
					OnFallbackFailure:  true,
					OnPolicyFailure:    true,
				},
			},
		},
		Dedupe: DedupeConfig{
			Enabled: false,
			Key:     "event_id",
			Window:  24 * time.Hour,
			Backend: "memory",
		},
		Schema: SchemaGovernanceConfig{
			Mode:          "off",
			SchemaVersion: "v1",
			EventVersion:  "v1",
			Registry: []SchemaRegistryEntry{
				{
					SchemaVersion: "v1",
					EventVersion:  "v1",
					RequiredFields: []string{
						"schema_version",
						"event_version",
						"event_id",
						"event",
						"timestamp",
					},
				},
			},
			QuarantinePath: "loxa-quarantine.ndjson",
		},
	}
}

func Load(data []byte, cfg *Config) error {
	return yaml.Unmarshal(data, cfg)
}

// LoadFile reads and parses a YAML config file into cfg.
func LoadFile(cfg *Config, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}
	return nil
}
