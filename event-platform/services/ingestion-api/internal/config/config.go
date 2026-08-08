package config

import "os"

const (
	DefaultListenAddr     = ":8080"
	DefaultKafkaBroker    = "kafka:9092"
	DefaultRedisAddr      = "redis:6379"
	DefaultOTLPEndpoint   = "otel-collector:4317"
	DefaultEventTypesPath = "/etc/config/event-types.yaml"
	DefaultMaxConcurrent  = 2000
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
	return Config{
		ListenAddr:     getEnv("LISTEN_ADDR", DefaultListenAddr),
		KafkaBrokers:   []string{getEnv("KAFKA_BROKERS", DefaultKafkaBroker)},
		RedisAddr:      getEnv("REDIS_ADDR", DefaultRedisAddr),
		OTLPEndpoint:   getEnv("OTLP_ENDPOINT", DefaultOTLPEndpoint),
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
