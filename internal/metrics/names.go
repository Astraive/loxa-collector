package metrics

const (
	RequestsTotalName       = "loxa_collector_requests_total"
	RequestsAuthErrorsName  = "loxa_collector_requests_auth_errors_total"
	RequestsLimitedName     = "loxa_collector_requests_limited_total"
	EventsAcceptedName      = "loxa_collector_events_accepted_total"
	EventsInvalidName       = "loxa_collector_events_invalid_total"
	EventsRejectedName      = "loxa_collector_events_rejected_total"
	EventsDedupedName       = "loxa_collector_events_deduped_total"
	SinkWriteErrorsName     = "loxa_collector_sink_write_errors_total"
	SpoolBytesName          = "loxa_collector_spool_bytes"
	SinkHealthName          = "loxa_collector_sink_health"
	DiskHealthName          = "loxa_collector_disk_health"
	SpoolHealthName         = "loxa_collector_spool_health"
	ReadyName               = "loxa_collector_ready"
)
