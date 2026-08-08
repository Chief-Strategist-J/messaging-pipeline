package eventtypes

type FieldRule struct {
	Field     string `yaml:"field"`
	Required  bool   `yaml:"required"`
	MaxLength int    `yaml:"max_length"`
}

type EventTypeConfig struct {
	Name            string      `yaml:"name"`
	Topic           string      `yaml:"topic"`
	PayloadRules    []FieldRule `yaml:"payload_validation"`
	CustomProcessor string      `yaml:"custom_processor"`
}

type Config struct {
	EventTypes []EventTypeConfig `yaml:"event_types"`
}
