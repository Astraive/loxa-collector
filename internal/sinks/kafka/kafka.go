package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	collectorevent "github.com/astraive/loxa-collector/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config controls Kafka sink behavior.
type Config struct {
	Brokers []string
	Topic   string
	// TLSConfig enables TLS for Kafka broker connections when provided.
	TLSConfig *tls.Config
}

type sink struct {
	client *kgo.Client
	topic  string
}

// New creates a Kafka sink.
func New(cfg Config) (collectorevent.Sink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: Brokers is required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka: Topic is required")
	}

	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.TLSConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLSConfig.Clone()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &sink{client: client, topic: cfg.Topic}, nil
}

func (s *sink) Name() string { return "kafka" }

func (s *sink) WriteEvent(ctx context.Context, encoded []byte, _ *collectorevent.Event) error {
	rec := &kgo.Record{Topic: s.topic, Value: encoded}
	res := s.client.ProduceSync(ctx, rec)
	return res.FirstErr()
}

func (s *sink) Flush(ctx context.Context) error {
	return s.client.Flush(ctx)
}

func (s *sink) Close(_ context.Context) error {
	s.client.Close()
	return nil
}
