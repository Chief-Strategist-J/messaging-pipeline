package events

import (
	"context"
	"fmt"
	"os"

	"github.com/buger/jsonparser"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"gopkg.in/yaml.v3"
)

var registry = map[string]EventTypeConfig{}

func LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cfg RegistryConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	return LoadFromConfig(cfg)
}

func LoadFromConfig(cfg RegistryConfig) error {
	for _, et := range cfg.EventTypes {
		if et.Name == "" {
			return fmt.Errorf("event type entry missing name")
		}
		if et.Topic == "" {
			return fmt.Errorf("event type %q missing topic", et.Name)
		}
		registry[et.Name] = et
	}
	return nil
}

func Get(name string) (EventTypeConfig, bool) {
	cfg, ok := registry[name]
	return cfg, ok
}

type CustomProcessor func(ctx context.Context, payloadJSON []byte) ([]byte, error)

var customProcessors = map[string]CustomProcessor{}

func RegisterCustomProcessor(name string, fn CustomProcessor) {
	customProcessors[name] = fn
}

func GetCustomProcessor(name string) (CustomProcessor, bool) {
	if name == "" {
		return nil, false
	}
	fn, ok := customProcessors[name]
	return fn, ok
}

func ValidatePayload(ctx context.Context, cfg EventTypeConfig, payloadJSON []byte) error {
	_, span := otel.Tracer("events").Start(ctx, "events.ValidatePayload")
	defer span.End()

	span.SetAttributes(
		attribute.String("event.type", cfg.Name),
		attribute.Int("event.payload.size", len(payloadJSON)),
	)

	for _, rule := range cfg.PayloadRules {
		val, dataType, _, err := jsonparser.Get(payloadJSON, rule.Field)
		if rule.Required && (err != nil || dataType == jsonparser.NotExist) {
			return fmt.Errorf("payload.%s is required", rule.Field)
		}
		if rule.MaxLength > 0 && len(val) > rule.MaxLength {
			return fmt.Errorf("payload.%s exceeds max length %d", rule.Field, rule.MaxLength)
		}
	}
	return nil
}
