package tests

import (
	"testing"

	"event-platform/ingestion-api/src/features/events"
)

func TestRawEventValidate(t *testing.T) {
	// Test valid envelope
	evt := events.RawEvent{
		EventID:   "evt-123",
		EventType: "heartbeat",
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("expected valid envelope, got error: %v", err)
	}

	// Test missing event_id
	evtNoID := events.RawEvent{
		EventType: "heartbeat",
	}
	if err := evtNoID.Validate(); err == nil {
		t.Fatal("expected error for missing event_id")
	}

	// Test missing event_type
	evtNoType := events.RawEvent{
		EventID: "evt-123",
	}
	if err := evtNoType.Validate(); err == nil {
		t.Fatal("expected error for missing event_type")
	}
}

func TestLoadFromConfigAndGet(t *testing.T) {
	cfg := events.RegistryConfig{
		EventTypes: []events.EventTypeConfig{
			{
				Name:  "test_event",
				Topic: "test_topic",
				PayloadRules: []events.FieldRule{
					{Field: "url", Required: true, MaxLength: 100},
				},
			},
		},
	}

	err := events.FeatureLoadFromConfig(cfg)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	registered, ok := events.FeatureGet("test_event")
	if !ok {
		t.Fatal("expected test_event to be registered")
	}
	if registered.Topic != "test_topic" {
		t.Errorf("expected topic test_topic, got %s", registered.Topic)
	}
}

func TestValidatePayload(t *testing.T) {
	cfg := events.EventTypeConfig{
		Name:  "test_event",
		Topic: "test_topic",
		PayloadRules: []events.FieldRule{
			{Field: "amount", Required: true},
			{Field: "currency", Required: false, MaxLength: 3},
		},
	}

	// Valid payload
	payload := []byte(`{"amount": 100, "currency": "USD"}`)
	if err := events.FeatureValidatePayload(cfg, payload); err != nil {
		t.Fatalf("expected payload to be valid, got: %v", err)
	}

	// Missing required field
	payloadMissing := []byte(`{"currency": "USD"}`)
	if err := events.FeatureValidatePayload(cfg, payloadMissing); err == nil {
		t.Fatal("expected error for missing required field")
	}

	// Exceeds max length
	payloadTooLong := []byte(`{"amount": 100, "currency": "USDOLLARS"}`)
	if err := events.FeatureValidatePayload(cfg, payloadTooLong); err == nil {
		t.Fatal("expected error for max length violation")
	}
}

func TestPurchaseEnrichment(t *testing.T) {
	payload := []byte(`{"amount_cents": 1000, "currency": "usd"}`)
	enriched, err := events.FeaturePurchaseEnrichment(payload)
	if err != nil {
		t.Fatalf("enrichment failed: %v", err)
	}

	expected := `{"amount_cents": 1000, "currency": "USD"}`
	if string(enriched) != expected {
		t.Errorf("expected %s, got %s", expected, string(enriched))
	}
}
