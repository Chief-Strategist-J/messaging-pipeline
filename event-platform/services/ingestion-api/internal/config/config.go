package config

import (
	"os"
	"strconv"
)

const (
	DefaultListenAddr     = ":8080"
	DefaultKafkaBroker    = "kafka:9092"
	DefaultRedisAddr      = "redis:6379"
	DefaultOTLPEndpoint   = "otel-collector:4317"
	DefaultEventTypesPath = "/etc/config/event-types.yaml"
	DefaultMaxConcurrent  = 2000
	DefaultSchemaID       = 1
)

type Config struct {
	ListenAddr     string
	KafkaBrokers   []string
	RedisAddr      string
	OTLPEndpoint   string
	SchemaID       uint32
	EventTypesPath string
	MaxConcurrent  int
}

func Load() Config {
	schemaID := uint32(DefaultSchemaID)
	if s := os.Getenv("SCHEMA_ID"); s != "" {
		if id, err := strconv.ParseUint(s, 10, 32); err == nil {
			schemaID = uint32(id)
		}
	}
	return Config{
		ListenAddr:     getEnv("LISTEN_ADDR", DefaultListenAddr),
		KafkaBrokers:   []string{getEnv("KAFKA_BROKERS", DefaultKafkaBroker)},
		RedisAddr:      getEnv("REDIS_ADDR", DefaultRedisAddr),
		OTLPEndpoint:   getEnv("OTLP_ENDPOINT", DefaultOTLPEndpoint),
		SchemaID:       schemaID,
		EventTypesPath: getEnv("EVENT_TYPES_PATH", DefaultEventTypesPath),
		MaxConcurrent:  DefaultMaxConcurrent,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
