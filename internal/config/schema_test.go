package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerConfigDefaults(t *testing.T) {
	cfg := Default()

	assert.True(t, cfg.Collector.Server.HTTP.Enabled)
	assert.Equal(t, ":9090", cfg.Collector.Server.HTTP.Addr)
	assert.Equal(t, 5*time.Second, cfg.Collector.Server.HTTP.ReadHeaderTimeout)
	assert.Equal(t, int64(10*1024*1024), cfg.Collector.Server.HTTP.MaxBodyBytes)

	assert.False(t, cfg.Collector.Server.GRPC.Enabled)
	assert.Equal(t, ":9091", cfg.Collector.Server.GRPC.Port)
	assert.Equal(t, 1000, cfg.Collector.Server.GRPC.MaxConnections)
	assert.Equal(t, 100, cfg.Collector.Server.GRPC.MaxConcurrentStreams)

	assert.False(t, cfg.Collector.Server.GraphQL.Enabled)
	assert.Equal(t, ":9092", cfg.Collector.Server.GraphQL.Port)
	assert.True(t, cfg.Collector.Server.GraphQL.Playground)
}

func TestServerConfigYAML(t *testing.T) {
	yaml := `
collector:
  addr: ":9090"
  server:
    http:
      enabled: true
      addr: ":9090"
      read_header_timeout: 10s
      max_body_bytes: 20971520
    grpc:
      enabled: true
      port: ":9091"
      max_connections: 500
      max_concurrent_streams: 50
      keepalive:
        max_connection_age: 10m
        time: 5m
    graphql:
      enabled: true
      port: ":9092"
      playground: true
      depth_limit: 15
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "loxa.yaml")
	err := os.WriteFile(configFile, []byte(yaml), 0644)
	require.NoError(t, err)

	cfg := Default()
	err = LoadFile(&cfg, configFile)
	require.NoError(t, err)

	assert.True(t, cfg.Collector.Server.HTTP.Enabled)
	assert.Equal(t, ":9090", cfg.Collector.Server.HTTP.Addr)
	assert.Equal(t, 10*time.Second, cfg.Collector.Server.HTTP.ReadHeaderTimeout)
	assert.Equal(t, int64(20971520), cfg.Collector.Server.HTTP.MaxBodyBytes)

	assert.True(t, cfg.Collector.Server.GRPC.Enabled)
	assert.Equal(t, ":9091", cfg.Collector.Server.GRPC.Port)
	assert.Equal(t, 500, cfg.Collector.Server.GRPC.MaxConnections)
	assert.Equal(t, 50, cfg.Collector.Server.GRPC.MaxConcurrentStreams)
	assert.Equal(t, 10*time.Minute, cfg.Collector.Server.GRPC.Keepalive.MaxConnectionAge)
	assert.Equal(t, 5*time.Minute, cfg.Collector.Server.GRPC.Keepalive.Time)

	assert.True(t, cfg.Collector.Server.GraphQL.Enabled)
	assert.Equal(t, ":9092", cfg.Collector.Server.GraphQL.Port)
	assert.True(t, cfg.Collector.Server.GraphQL.Playground)
	assert.Equal(t, 15, cfg.Collector.Server.GraphQL.DepthLimit)
}

func TestServerConfigTLS(t *testing.T) {
	yaml := `
collector:
  server:
    grpc:
      enabled: true
      port: ":9091"
      tls:
        enabled: true
        cert_file: "/path/to/cert.pem"
        key_file: "/path/to/key.pem"
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "loxa.yaml")
	err := os.WriteFile(configFile, []byte(yaml), 0644)
	require.NoError(t, err)

	cfg := Default()
	err = LoadFile(&cfg, configFile)
	require.NoError(t, err)

	assert.True(t, cfg.Collector.Server.GRPC.TLS.Enabled)
	assert.Equal(t, "/path/to/cert.pem", cfg.Collector.Server.GRPC.TLS.CertFile)
	assert.Equal(t, "/path/to/key.pem", cfg.Collector.Server.GRPC.TLS.KeyFile)
}