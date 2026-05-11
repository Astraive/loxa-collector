# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-05-11

### Horizon 1 — Foundation Gate
- Updated README "Known Limitations" to reflect that spool, retry, DLQ, and dedupe are available in the current release.
- Moved remaining hardcoded collector values to config fields:
  - `duckdb.column_types` for schema column type mapping.
  - `reliability.spool_file` for WAL file name.
  - `reliability.delivery_queue_size` for spool delivery queue.
- Reference config `build/loxa-collector.yaml` updated with new fields.
- Strict gate: `go test ./... -race` green across all 9 modules; `go vet` clean.

### Added

- Multi-module workspace split with `go.work`:
  - core (`github.com/astraive/loxa-go`)
  - optional middleware (`github.com/astraive/loxa-go/middleware`)
  - optional integrations (`github.com/astraive/loxa-go/integrations`)
  - optional sinks (`github.com/astraive/loxa-go/sinks`)
  - proto/contracts (`github.com/astraive/loxa-go/proto`)
- New `testkit` package for test capture/assert helpers.
- Dependency boundary conformance test for root module.
- Example set for net/http, slog bridge, custom schema, OTLP, and Kafka.

### Changed

- Root module dependency graph slimmed to core-only requirements.
- Integration DuckDB stress test uses temp DB path to avoid cross-run file lock collisions.
- README/docs repositioned LOXA as canonical wide-event layer.

### Breaking

- Removed `loxa.Capture` and `loxa.AssertEvent`; use `github.com/astraive/loxa-go/testkit`.
- Removed root middleware wrapper; use `github.com/astraive/loxa-go/middleware/nethttp`.
