package main

import (
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	collectorconfig "github.com/astraive/loxa-collector/internal/config"
	processing "github.com/astraive/loxa-collector/internal/processing"
	"github.com/astraive/loxa-go"
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
	queueDir                string
	queueBatchSize          int
	queueBatchTimeout       time.Duration
	queueFlushInterval      time.Duration
	queueCircuitThreshold   int
	queueCircuitTimeout     time.Duration
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
	kafkaAcks               string
	kafkaRequestTimeout    time.Duration
	kafkaIdempotence       bool
	kafkaMaxRetries        int
	kafkaRetryBackoff      time.Duration
	workerConsumerGroup     string
	workerPollTimeout       time.Duration
	schemaMode              string
	schemaSchemaVersion     string
	schemaEventVersion      string
	schemaQuarantinePath    string
	schemaRegistry          []collectorconfig.SchemaRegistryEntry
}

type collectorMetrics struct {
	requestsTotal     atomic.Int64
	requestsAuthErr   atomic.Int64
	requestsLimited   atomic.Int64
	eventsAccepted    atomic.Int64
	eventsInvalid     atomic.Int64
	eventsRejected    atomic.Int64
	eventsDeduped     atomic.Int64
	sinkWriteErrors  atomic.Int64
	spoolBytes       atomic.Int64
	retryAttempts    atomic.Int64
	spoolReplayCount int64
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
	spoolPosFile   string
	spoolProcessedPos int64
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