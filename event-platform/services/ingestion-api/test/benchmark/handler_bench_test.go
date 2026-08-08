package benchmark

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"context"

	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/httpapi"
	"event-platform/ingestion-api/internal/ingest"
)

type noopProducer struct{}

func (noopProducer) Produce(_ context.Context, _ string, _ ingest.RawEvent) error { return nil }
func (noopProducer) Close()                                                        {}

type noopDeduper struct{}

func (noopDeduper) SeenBefore(_ context.Context, _ string) (bool, error) { return false, nil }

func BenchmarkHandlerHappyPath(b *testing.B) {
	_ = eventtypes.LoadFromConfig(eventtypes.Config{
		EventTypes: []eventtypes.EventTypeConfig{
			{Name: "heartbeat", Topic: "events.raw"},
		},
	})

	handler := httpapi.NewHandler(noopProducer{}, noopDeduper{})
	body := `{"event_id":"bench-001","event_type":"heartbeat","payload":"{}"}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}
