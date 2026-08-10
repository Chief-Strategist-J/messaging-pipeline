package events

import (
	"errors"
	"time"
)

type RawEvent struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	OccurredAt int64  `json:"occurred_at"`
	Payload    string `json:"payload"`
}

func (e *RawEvent) Validate() error {
	if e.EventID == "" {
		return errors.New("event_id is required")
	}
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	if e.OccurredAt == 0 {
		e.OccurredAt = time.Now().UnixMilli()
	}
	return nil
}

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

type RegistryConfig struct {
	EventTypes []EventTypeConfig `yaml:"event_types"`
}
