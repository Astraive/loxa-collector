package main

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type dedupeStore interface {
	SeenBefore(ctx context.Context, value string, window time.Duration) (bool, error)
	Close() error
}

type redisDedupeStore struct {
	client *redis.Client
	prefix string
}

func newRedisDedupeStore(cfg collectorConfig) dedupeStore {
	if !strings.EqualFold(strings.TrimSpace(cfg.dedupeBackend), "redis") || strings.TrimSpace(cfg.dedupeRedisAddr) == "" {
		return nil
	}
	return &redisDedupeStore{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.dedupeRedisAddr,
			Password: cfg.dedupeRedisPassword,
			DB:       cfg.dedupeRedisDB,
		}),
		prefix: cfg.dedupeRedisPrefix,
	}
}

func (s *redisDedupeStore) SeenBefore(ctx context.Context, value string, window time.Duration) (bool, error) {
	key := value
	if strings.TrimSpace(s.prefix) != "" {
		key = s.prefix + value
	}
	ok, err := s.client.SetNX(ctx, key, "1", window).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func (s *redisDedupeStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}
