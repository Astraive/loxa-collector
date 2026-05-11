package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDefaultCoreSchema(t *testing.T) {
	cfg := Default()

	if cfg.Collector.Addr != ":9090" || cfg.Collector.MaxEventsPerReq <= 0 {
		t.Fatalf("unexpected collector defaults: %+v", cfg.Collector)
	}
	if cfg.Auth.Header == "" || cfg.Auth.ValueEnv == "" {
		t.Fatalf("unexpected auth defaults: %+v", cfg.Auth)
	}
	if !cfg.RateLimit.Enabled || cfg.RateLimit.RPS <= 0 || cfg.RateLimit.Burst <= 0 {
		t.Fatalf("unexpected rate limit defaults: %+v", cfg.RateLimit)
	}
	if cfg.Routes.Ingest == "" || cfg.Routes.Health == "" || cfg.Routes.Ready == "" || cfg.Routes.Metrics == "" {
		t.Fatalf("unexpected routes defaults: %+v", cfg.Routes)
	}
	if cfg.Storage.Primary != "duckdb" {
		t.Fatalf("unexpected storage default: %+v", cfg.Storage)
	}
	if cfg.DuckDB.Path == "" || cfg.DuckDB.Driver == "" || cfg.DuckDB.Table == "" {
		t.Fatalf("unexpected duckdb defaults: %+v", cfg.DuckDB)
	}
	if cfg.Logging.Level == "" || cfg.Logging.Format == "" {
		t.Fatalf("unexpected logging defaults: %+v", cfg.Logging)
	}
	if !cfg.Metrics.Prometheus {
		t.Fatalf("unexpected metrics defaults: %+v", cfg.Metrics)
	}
}

func TestYAMLUnmarshalCoreSchema(t *testing.T) {
	raw := `
collector:
  addr: ":9191"
  read_header_timeout: 2s
auth:
  enabled: true
  header: X-Key
  value_env: MY_KEY
rate_limit:
  enabled: false
  rps: 120.5
  burst: 200
routes:
  ingest: /in
  health: /h
  ready: /r
  metrics: /m
storage:
  primary: duckdb
duckdb:
  path: collector.db
  table: events_custom
logging:
  level: debug
  format: json
metrics:
  prometheus: false
`

	cfg := Default()
	if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	if cfg.Collector.Addr != ":9191" || cfg.Collector.ReadHeaderTimeout != 2*time.Second {
		t.Fatalf("collector values not typed correctly: %+v", cfg.Collector)
	}
	if !cfg.Auth.Enabled || cfg.Auth.Header != "X-Key" || cfg.Auth.ValueEnv != "MY_KEY" {
		t.Fatalf("auth values not typed correctly: %+v", cfg.Auth)
	}
	if cfg.RateLimit.Enabled || cfg.RateLimit.RPS != 120.5 || cfg.RateLimit.Burst != 200 {
		t.Fatalf("rate_limit values not typed correctly: %+v", cfg.RateLimit)
	}
	if cfg.Routes.Ingest != "/in" || cfg.Storage.Primary != "duckdb" {
		t.Fatalf("routes/storage values not typed correctly: %+v %+v", cfg.Routes, cfg.Storage)
	}
	if cfg.DuckDB.Path != "collector.db" || cfg.DuckDB.Table != "events_custom" {
		t.Fatalf("duckdb values not typed correctly: %+v", cfg.DuckDB)
	}
	if cfg.Logging.Level != "debug" || cfg.Logging.Format != "json" || cfg.Metrics.Prometheus {
		t.Fatalf("logging/metrics values not typed correctly: %+v %+v", cfg.Logging, cfg.Metrics)
	}
}
