package benchmark

import (
	"bytes"
	"encoding/json"
	"testing"

	"event-platform/ingestion-api/internal/ingest"
	"github.com/buger/jsonparser"
)

func BenchmarkRawEventValidate(b *testing.B) {
	evt := ingest.RawEvent{
		EventID:    "bench-id-001",
		EventType:  "page_view",
		OccurredAt: 1700000000000,
		Payload:    json.RawMessage(`{"url":"https://example.com/path"}`),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = evt.Validate()
	}
}

func BenchmarkRawEventValidateWithAutoTimestamp(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evt := ingest.RawEvent{EventID: "bench-id", EventType: "test"}
		_ = evt.Validate()
	}
}

func BenchmarkDecodeRawEvent_500KB(b *testing.B) {
	padding := make([]byte, 500*1024)
	for i := range padding {
		padding[i] = 'x'
	}
	innerPayload := `{"url":"/home","data":"` + string(padding) + `"}`
	bodyBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":    "bench-envelope-001",
		"event_type":  "page_view",
		"occurred_at": 1700000000000,
		"payload":     innerPayload,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var evt ingest.RawEvent
		_ = json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&evt)
	}
}

func BenchmarkDecodeRawEvent_500KB_Jsonparser(b *testing.B) {
	padding := make([]byte, 500*1024)
	for i := range padding {
		padding[i] = 'x'
	}
	innerPayload := `{"url":"/home","data":"` + string(padding) + `"}`
	bodyBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":    "bench-envelope-001",
		"event_type":  "page_view",
		"occurred_at": 1700000000000,
		"payload":     innerPayload,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eventID, _ := jsonparser.GetString(bodyBytes, "event_id")
		eventType, _ := jsonparser.GetString(bodyBytes, "event_type")
		occurredAt, _ := jsonparser.GetInt(bodyBytes, "occurred_at")
		payload, _, _, _ := jsonparser.Get(bodyBytes, "payload")
		evt := ingest.RawEvent{
			EventID:    eventID,
			EventType:  eventType,
			OccurredAt: occurredAt,
			Payload:    json.RawMessage(payload),
		}
		_ = evt
	}
}
