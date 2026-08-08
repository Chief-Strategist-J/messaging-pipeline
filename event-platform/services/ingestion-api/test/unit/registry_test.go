package unit

import (
	"testing"

	"event-platform/ingestion-api/internal/eventtypes"
)

func setupRegistry(t *testing.T, cfgs []eventtypes.EventTypeConfig) {
	t.Helper()
	err := eventtypes.LoadFromConfig(eventtypes.Config{EventTypes: cfgs})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
}

func TestLoadFromConfigValid(t *testing.T) {
	setupRegistry(t, []eventtypes.EventTypeConfig{
		{Name: "heartbeat", Topic: "events.raw"},
		{Name: "page_view", Topic: "events.raw", PayloadRules: []eventtypes.FieldRule{{Field: "url", Required: true}}},
	})

	got, ok := eventtypes.Get("heartbeat")
	if !ok {
		t.Fatal("expected heartbeat to be registered")
	}
	if got.Topic != "events.raw" {
		t.Errorf("expected topic events.raw, got %s", got.Topic)
	}

	got, ok = eventtypes.Get("page_view")
	if !ok {
		t.Fatal("expected page_view to be registered")
	}
	if len(got.PayloadRules) != 1 {
		t.Errorf("expected 1 payload rule, got %d", len(got.PayloadRules))
	}
}

func TestLoadFromConfigMissingName(t *testing.T) {
	err := eventtypes.LoadFromConfig(eventtypes.Config{
		EventTypes: []eventtypes.EventTypeConfig{{Topic: "events.raw"}},
	})
	if err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestLoadFromConfigMissingTopic(t *testing.T) {
	err := eventtypes.LoadFromConfig(eventtypes.Config{
		EventTypes: []eventtypes.EventTypeConfig{{Name: "heartbeat"}},
	})
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestGetUnregistered(t *testing.T) {
	_, ok := eventtypes.Get("nonexistent_type_xyz")
	if ok {
		t.Error("expected false for unregistered event type")
	}
}

func TestLoadFromConfigEmpty(t *testing.T) {
	err := eventtypes.LoadFromConfig(eventtypes.Config{EventTypes: []eventtypes.EventTypeConfig{}})
	if err != nil {
		t.Fatalf("unexpected error for empty config: %v", err)
	}
}
