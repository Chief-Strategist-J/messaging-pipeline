package benchmark

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/httpapi"
)

type noopProducer struct{}

func (noopProducer) Produce(_ interface{}, _ string, _ interface{}) error { return nil }
func (noopProducer) Close()                                               {}

type noopDeduper struct{}

func (noopDeduper) SeenBefore(_ interface{}, _ string) (bool, error) { return false, nil }

func BenchmarkHandlerHappyPath(b *testing.B) {
	_ = eventtypes.LoadFromConfig(eventtypes.Config{
		EventTypes: []eventtypes.EventTypeConfig{
			{Name: "heartbeat", Topic: "events.raw"},
		},
	})

	handler := httpapi.NewHandler(nil, nil)
	body := `{"event_id":"bench-001","event_type":"heartbeat","payload":"{}"}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}
