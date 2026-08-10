package events

import (
	"fmt"
	"os"

	"github.com/buger/jsonparser"
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

// CustomProcessor processes / enriches a raw payload
type CustomProcessor func(payloadJSON []byte) ([]byte, error)

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

// ValidatePayload validates a payload against EventTypeConfig using jsonparser
func ValidatePayload(cfg EventTypeConfig, payloadJSON []byte) error {
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
