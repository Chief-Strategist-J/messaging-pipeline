package unit

import (
	"os"
	"testing"

	"event-platform/ingestion-api/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	os.Clearenv()
	cfg := config.Load()

	if cfg.ListenAddr != ":8080" {
		t.Errorf("expected default ListenAddr :8080, got %s", cfg.ListenAddr)
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "kafka:9092" {
		t.Errorf("expected default KafkaBrokers [kafka:9092], got %v", cfg.KafkaBrokers)
	}
	if cfg.RedisAddr != "redis:6379" {
		t.Errorf("expected default RedisAddr redis:6379, got %s", cfg.RedisAddr)
	}
	if cfg.OTLPEndpoint != "otel-collector:4317" {
		t.Errorf("expected default OTLPEndpoint otel-collector:4317, got %s", cfg.OTLPEndpoint)
	}
	if cfg.EventTypesPath != "/etc/config/event-types.yaml" {
		t.Errorf("expected default EventTypesPath, got %s", cfg.EventTypesPath)
	}
	if cfg.MaxConcurrent != 2000 {
		t.Errorf("expected default MaxConcurrent 2000, got %d", cfg.MaxConcurrent)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("LISTEN_ADDR", ":9090")
	t.Setenv("KAFKA_BROKERS", "broker1:9092")
	t.Setenv("REDIS_ADDR", "redis-cluster:6380")
	t.Setenv("OTLP_ENDPOINT", "collector:4317")
	t.Setenv("EVENT_TYPES_PATH", "/custom/path.yaml")

	cfg := config.Load()

	if cfg.ListenAddr != ":9090" {
		t.Errorf("expected :9090, got %s", cfg.ListenAddr)
	}
	if cfg.KafkaBrokers[0] != "broker1:9092" {
		t.Errorf("expected broker1:9092, got %s", cfg.KafkaBrokers[0])
	}
	if cfg.RedisAddr != "redis-cluster:6380" {
		t.Errorf("expected redis-cluster:6380, got %s", cfg.RedisAddr)
	}
	if cfg.OTLPEndpoint != "collector:4317" {
		t.Errorf("expected collector:4317, got %s", cfg.OTLPEndpoint)
	}
	if cfg.EventTypesPath != "/custom/path.yaml" {
		t.Errorf("expected /custom/path.yaml, got %s", cfg.EventTypesPath)
	}
}
