package unit

import (
	"testing"

	"event-platform/ingestion-api/internal/ingest"
)

func TestRawEventValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   ingest.RawEvent
		wantErr bool
	}{
		{
			name:    "valid event",
			event:   ingest.RawEvent{EventID: "abc-123", EventType: "page_view", OccurredAt: 1700000000000, Payload: "{}"},
			wantErr: false,
		},
		{
			name:    "missing event_id",
			event:   ingest.RawEvent{EventType: "page_view", OccurredAt: 1700000000000},
			wantErr: true,
		},
		{
			name:    "missing event_type",
			event:   ingest.RawEvent{EventID: "abc-123", OccurredAt: 1700000000000},
			wantErr: true,
		},
		{
			name:    "zero occurred_at gets set automatically",
			event:   ingest.RawEvent{EventID: "abc-123", EventType: "page_view"},
			wantErr: false,
		},
		{
			name:    "empty strings for both required fields",
			event:   ingest.RawEvent{},
			wantErr: true,
		},
		{
			name:    "whitespace event_id treated as valid",
			event:   ingest.RawEvent{EventID: " ", EventType: "test"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRawEventValidateSetsOccurredAt(t *testing.T) {
	evt := ingest.RawEvent{EventID: "abc", EventType: "test"}
	if evt.OccurredAt != 0 {
		t.Fatal("expected OccurredAt to start at 0")
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evt.OccurredAt == 0 {
		t.Error("expected OccurredAt to be set after Validate()")
	}
}

func TestRawEventValidatePreservesExistingOccurredAt(t *testing.T) {
	const existingTS int64 = 1700000000000
	evt := ingest.RawEvent{EventID: "abc", EventType: "test", OccurredAt: existingTS}
	if err := evt.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evt.OccurredAt != existingTS {
		t.Errorf("expected OccurredAt to remain %d, got %d", existingTS, evt.OccurredAt)
	}
}
