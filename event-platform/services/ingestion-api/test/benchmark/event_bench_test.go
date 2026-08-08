package benchmark

import (
	"testing"

	"event-platform/ingestion-api/internal/ingest"
)

func BenchmarkRawEventValidate(b *testing.B) {
	evt := ingest.RawEvent{
		EventID:    "bench-id-001",
		EventType:  "page_view",
		OccurredAt: 1700000000000,
		Payload:    `{"url":"https://example.com/path"}`,
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
