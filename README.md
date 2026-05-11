# LOXA Collector

LOXA Collector is the runtime repository for LOXA ingestion, validation, durability, fanout, worker processing, and deployment assets.

Key binaries:

- `loxa-collector`: HTTP ingest server
- `loxa-worker`: queue consumer for distributed delivery
- `loxa-loadgen`: local load generator

Contract and SDK:

- event contract: `Astraive/loxa-spec`
- Go SDK: `Astraive/loxa-go`
- operations CLI: `Astraive/loxa-cli`

Local run example:

```bash
go run ./cmd/loxa-collector run -c configs/loxa.local.yaml
```
