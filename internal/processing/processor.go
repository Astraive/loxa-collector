package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/astraive/loxa-collector/internal/validation"
	"github.com/astraive/loxa-go"
)

var ErrInvalidEvent = errors.New("invalid event payload: expected JSON object")

type Config struct {
	DeliveryPolicy          string
	RetryEnabled            bool
	RetryMaxAttempts        int
	RetryInitialBackoff     time.Duration
	RetryMaxBackoff         time.Duration
	RetryJitter             bool
	FallbackEnabled         bool
	FallbackOnPrimaryFail   bool
	FallbackOnSecondaryFail bool
	FallbackOnPolicyFail    bool
	DLQEnabled              bool
	DLQPath                 string
	DLQOnPrimaryFail        bool
	DLQOnSecondaryFail      bool
	DLQOnFallbackFail       bool
	DLQOnPolicyFail         bool
	DedupeEnabled           bool
	DedupeKey               string
	DedupeWindow            time.Duration
	ValidateJSONObjects     bool
	OnDiskFull              func()
	Schema                  SchemaConfig
}

type SchemaConfig struct {
	Mode           string // off, warn, reject, quarantine
	SchemaVersion  string
	EventVersion   string
	Registry       []SchemaRegistryEntry
	QuarantinePath string
}

type SchemaRegistryEntry struct {
	SchemaVersion  string
	EventVersion   string
	RequiredFields []string
}

type NamedSink struct {
	Name string
	Sink loxa.Sink
}

type SinkDeliveryFailure struct {
	Name string
	Err  error
}

type DeliveryOutcome struct {
	PrimaryErr        error
	SecondaryFailures []SinkDeliveryFailure
	SecondarySuccess  int
	FallbackAttempted bool
	FallbackErr       error
	PolicyErr         error
}

func (o DeliveryOutcome) FailureCount() int {
	total := 0
	if o.PrimaryErr != nil {
		total++
	}
	total += len(o.SecondaryFailures)
	if o.FallbackErr != nil {
		total++
	}
	return total
}

func (o DeliveryOutcome) FirstError() error {
	if o.FallbackErr != nil {
		return o.FallbackErr
	}
	if o.PrimaryErr != nil {
		return o.PrimaryErr
	}
	if len(o.SecondaryFailures) > 0 {
		return o.SecondaryFailures[0].Err
	}
	if o.PolicyErr != nil {
		return o.PolicyErr
	}
	return nil
}

type Result struct {
	Accepted bool
	Invalid  bool
	Deduped  bool
	Outcome  DeliveryOutcome
	Err      error
}

type Processor struct {
	cfg            Config
	primarySink    loxa.Sink
	secondarySinks []NamedSink
	fallbackSink   *NamedSink
	rng            *rand.Rand
	dlqFile        *os.File
	dlqMu          sync.Mutex
	quarantineFile *os.File
	quarantineMu   sync.Mutex
	dedupeMu       sync.Mutex
	dedupeSeenAt   map[string]time.Time
}

func New(cfg Config, primary loxa.Sink, secondary []NamedSink, fallback *NamedSink, rng *rand.Rand) (*Processor, error) {
	if primary == nil {
		return nil, errors.New("primary sink is required")
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	p := &Processor{
		cfg:            cfg,
		primarySink:    primary,
		secondarySinks: append([]NamedSink(nil), secondary...),
		fallbackSink:   fallback,
		rng:            rng,
		dedupeSeenAt:   make(map[string]time.Time),
	}
	if cfg.DLQEnabled {
		if strings.TrimSpace(cfg.DLQPath) == "" {
			return nil, errors.New("dlq path is required when dlq is enabled")
		}
		if err := os.MkdirAll(filepath.Dir(cfg.DLQPath), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir dlq dir: %w", err)
		}
		dlq, err := os.OpenFile(cfg.DLQPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open dlq file: %w", err)
		}
		p.dlqFile = dlq
	}
	if cfg.Schema.Mode == "quarantine" {
		if strings.TrimSpace(cfg.Schema.QuarantinePath) == "" {
			return nil, errors.New("quarantine path is required when schema mode is quarantine")
		}
		if err := os.MkdirAll(filepath.Dir(cfg.Schema.QuarantinePath), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir quarantine dir: %w", err)
		}
		q, err := os.OpenFile(cfg.Schema.QuarantinePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open quarantine file: %w", err)
		}
		p.quarantineFile = q
	}
	return p, nil
}

func (p *Processor) Close() error {
	var errs []string
	if p.dlqFile != nil {
		if err := p.dlqFile.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if p.quarantineFile != nil {
		if err := p.quarantineFile.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (p *Processor) Process(ctx context.Context, raw []byte) Result {
	if p.cfg.ValidateJSONObjects && !validation.IsJSONObject(raw) {
		p.writeDLQ(raw, ErrInvalidEvent)
		return Result{Invalid: true, Err: ErrInvalidEvent}
	}

	if p.cfg.DedupeEnabled {
		eventID, ok := validation.ExtractStringPath(raw, p.cfg.DedupeKey)
		if ok && p.isDuplicate(eventID) {
			return Result{Accepted: true, Deduped: true}
		}
	}

	validated, err := p.validateSchema(raw)
	if err != nil {
		mode := strings.ToLower(p.cfg.Schema.Mode)
		switch mode {
		case "warn":
			// Log warning but proceed
			fmt.Fprintf(os.Stderr, "[loxa] schema validation warning: %v\n", err)
		case "reject":
			return Result{Invalid: true, Err: err}
		case "quarantine":
			p.writeQuarantine(raw, err)
			return Result{Accepted: true, Invalid: true, Err: err} // Accepted by collector but quarantined
		}
	}
	raw = validated

	outcome, err := p.deliverWithRetry(ctx, raw)
	if p.shouldWriteDLQ(outcome) {
		p.writeDLQ(raw, outcome.FirstError())
	}
	if err != nil {
		return Result{Outcome: outcome, Err: err}
	}
	return Result{Accepted: true, Outcome: outcome}
}

func (p *Processor) validateSchema(raw []byte) ([]byte, error) {
	mode := strings.ToLower(p.cfg.Schema.Mode)
	if mode == "" || mode == "off" {
		return raw, nil
	}

	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return raw, fmt.Errorf("schema validation: invalid JSON: %w", err)
	}

	modified := false
	sv, _ := data["schema_version"].(string)
	ev, _ := data["event_version"].(string)

	if sv == "" && p.cfg.Schema.SchemaVersion != "" {
		sv = p.cfg.Schema.SchemaVersion
		data["schema_version"] = sv
		modified = true
	}
	if ev == "" && p.cfg.Schema.EventVersion != "" {
		ev = p.cfg.Schema.EventVersion
		data["event_version"] = ev
		modified = true
	}

	if len(p.cfg.Schema.Registry) > 0 {
		found := false
		var entry SchemaRegistryEntry
		for _, e := range p.cfg.Schema.Registry {
			if e.SchemaVersion == sv && e.EventVersion == ev {
				found = true
				entry = e
				break
			}
		}

		if found {
			for _, field := range entry.RequiredFields {
				if _, ok := data[field]; !ok {
					return raw, fmt.Errorf("schema validation: missing required field %q", field)
				}
			}
		} else if mode == "reject" || mode == "quarantine" {
			return raw, fmt.Errorf("schema validation: unknown schema/event version combination (%s/%s)", sv, ev)
		}
	}

	if modified {
		newRaw, err := json.Marshal(data)
		if err != nil {
			return raw, err
		}
		return newRaw, nil
	}
	return raw, nil
}

func (p *Processor) writeQuarantine(raw []byte, err error) {
	if p.quarantineFile == nil {
		return
	}
	errMsg := "schema_validation_failed"
	if err != nil {
		errMsg = err.Error()
	}
	record := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"error":     errMsg,
		"raw":       string(raw),
	}
	b, mErr := json.Marshal(record)
	if mErr != nil {
		return
	}
	p.quarantineMu.Lock()
	defer p.quarantineMu.Unlock()
	_, _ = p.quarantineFile.Write(append(b, '\n'))
}

func (p *Processor) normalizedDeliveryPolicy() string {
	policy := strings.ToLower(strings.TrimSpace(p.cfg.DeliveryPolicy))
	if policy == "" {
		return "require_primary"
	}
	return policy
}

func (p *Processor) evaluatePolicy(outcome *DeliveryOutcome) {
	primaryOK := outcome.PrimaryErr == nil
	switch p.normalizedDeliveryPolicy() {
	case "best_effort":
		if primaryOK || outcome.SecondarySuccess > 0 {
			outcome.PolicyErr = nil
			return
		}
	case "require_all":
		if primaryOK && len(outcome.SecondaryFailures) == 0 && outcome.SecondarySuccess == len(p.secondarySinks) {
			outcome.PolicyErr = nil
			return
		}
	default:
		if primaryOK {
			outcome.PolicyErr = nil
			return
		}
	}

	outcome.PolicyErr = fmt.Errorf(
		"delivery policy %s failed (primary_failed=%t secondary_failures=%d secondary_success=%d)",
		p.normalizedDeliveryPolicy(),
		outcome.PrimaryErr != nil,
		len(outcome.SecondaryFailures),
		outcome.SecondarySuccess,
	)
}

func (p *Processor) shouldAttemptFallback(outcome DeliveryOutcome) bool {
	if !p.cfg.FallbackEnabled || p.fallbackSink == nil {
		return false
	}
	if outcome.PrimaryErr != nil && p.cfg.FallbackOnPrimaryFail {
		return true
	}
	if len(outcome.SecondaryFailures) > 0 && p.cfg.FallbackOnSecondaryFail {
		return true
	}
	if outcome.PolicyErr != nil && p.cfg.FallbackOnPolicyFail {
		return true
	}
	return false
}

func (p *Processor) shouldWriteDLQ(outcome DeliveryOutcome) bool {
	if !p.cfg.DLQEnabled {
		return false
	}
	if outcome.PrimaryErr != nil && p.cfg.DLQOnPrimaryFail {
		return true
	}
	if len(outcome.SecondaryFailures) > 0 && p.cfg.DLQOnSecondaryFail {
		return true
	}
	if outcome.FallbackErr != nil && p.cfg.DLQOnFallbackFail {
		return true
	}
	if outcome.PolicyErr != nil && p.cfg.DLQOnPolicyFail {
		return true
	}
	return false
}

func (p *Processor) runDeliveryAttempt(ctx context.Context, raw []byte) DeliveryOutcome {
	outcome := DeliveryOutcome{}
	if err := p.primarySink.WriteEvent(ctx, raw, nil); err != nil {
		outcome.PrimaryErr = err
		if isDiskFullErr(err) && p.cfg.OnDiskFull != nil {
			p.cfg.OnDiskFull()
		}
	}

	for _, secondary := range p.secondarySinks {
		if err := secondary.Sink.WriteEvent(ctx, raw, nil); err != nil {
			outcome.SecondaryFailures = append(outcome.SecondaryFailures, SinkDeliveryFailure{Name: secondary.Name, Err: err})
			if isDiskFullErr(err) && p.cfg.OnDiskFull != nil {
				p.cfg.OnDiskFull()
			}
			continue
		}
		outcome.SecondarySuccess++
	}

	p.evaluatePolicy(&outcome)

	if p.shouldAttemptFallback(outcome) {
		outcome.FallbackAttempted = true
		if err := p.fallbackSink.Sink.WriteEvent(ctx, raw, nil); err != nil {
			outcome.FallbackErr = err
			if isDiskFullErr(err) && p.cfg.OnDiskFull != nil {
				p.cfg.OnDiskFull()
			}
		}
	}

	return outcome
}

func (p *Processor) deliverWithRetry(ctx context.Context, raw []byte) (DeliveryOutcome, error) {
	attempts := 1
	if p.cfg.RetryEnabled {
		attempts = p.cfg.RetryMaxAttempts
		if attempts <= 0 {
			attempts = 1
		}
	}
	backoff := p.cfg.RetryInitialBackoff
	maxBackoff := p.cfg.RetryMaxBackoff

	var last DeliveryOutcome
	for i := 0; i < attempts; i++ {
		last = p.runDeliveryAttempt(ctx, raw)
		if last.PolicyErr == nil {
			return last, nil
		}
		if i == attempts-1 {
			break
		}
		sleep := backoff
		if sleep > 0 && p.cfg.RetryJitter {
			sleep = time.Duration(float64(sleep) * (0.8 + 0.4*p.rng.Float64()))
		}
		if sleep > 0 {
			time.Sleep(sleep)
		}
		if backoff > 0 {
			backoff *= 2
		}
		if maxBackoff > 0 && backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	if last.PolicyErr == nil {
		last.PolicyErr = errors.New("delivery failed")
	}
	return last, last.PolicyErr
}

func (p *Processor) writeDLQ(raw []byte, err error) {
	if !p.cfg.DLQEnabled || p.dlqFile == nil {
		return
	}
	errMsg := "delivery_failed"
	if err != nil {
		errMsg = err.Error()
	}
	record := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"error":     errMsg,
		"raw":       string(raw),
	}
	b, mErr := json.Marshal(record)
	if mErr != nil {
		return
	}
	p.dlqMu.Lock()
	defer p.dlqMu.Unlock()
	_, _ = p.dlqFile.Write(append(b, '\n'))
}

func (p *Processor) isDuplicate(value string) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	p.dedupeMu.Lock()
	defer p.dedupeMu.Unlock()
	now := time.Now()
	for k, ts := range p.dedupeSeenAt {
		if now.Sub(ts) > p.cfg.DedupeWindow {
			delete(p.dedupeSeenAt, k)
		}
	}
	if ts, ok := p.dedupeSeenAt[value]; ok && now.Sub(ts) <= p.cfg.DedupeWindow {
		return true
	}
	p.dedupeSeenAt[value] = now
	return false
}

func isDiskFullErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "no space left") || strings.Contains(s, "disk full")
}
