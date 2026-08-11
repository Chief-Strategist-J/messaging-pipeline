package tests

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "event-platform/ingestion-api/src/api/rest/v1"
	"event-platform/ingestion-api/src/features/events"
)

type mockProducer struct {
	produced []string
}

func (m *mockProducer) Produce(ctx context.Context, topic string, eventID string, eventType string, occurredAt int64, payload []byte) error {
	m.produced = append(m.produced, eventID)
	return nil
}

func (m *mockProducer) Close() {}

type mockDeduper struct {
	seen map[string]bool
}

func (m *mockDeduper) SeenBefore(ctx context.Context, eventID string) (bool, error) {
	if m.seen[eventID] {
		return true, nil
	}
	m.seen[eventID] = true
	return false, nil
}

func TestEventsApiIntegration(t *testing.T) {
	cfg := events.RegistryConfig{
		EventTypes: []events.EventTypeConfig{
			{
				Name:  "page_view",
				Topic: "events.raw",
				PayloadRules: []events.FieldRule{
					{Field: "url", Required: true},
				},
			},
		},
	}
	_ = events.FeatureLoadFromConfig(cfg)

	producer := &mockProducer{produced: []string{}}
	deduper := &mockDeduper{seen: map[string]bool{}}

	handler := v1.NewHandler(producer, deduper)

	reqBody := `{"event_id":"id-123","event_type":"page_view","occurred_at":1718020000000,"payload":"{\"url\":\"http://test.com\"}"}`
	req := httptest.NewRequest("POST", "/v1/events", bytes.NewBufferString(reqBody))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected StatusAccepted, got %d, body: %s", rec.Code, rec.Body.String())
	}

	if len(producer.produced) != 1 || producer.produced[0] != "id-123" {
		t.Errorf("expected event id-123 to be produced, got %v", producer.produced)
	}

	reqDup := httptest.NewRequest("POST", "/v1/events", bytes.NewBufferString(reqBody))
	recDup := httptest.NewRecorder()

	handler.ServeHTTP(recDup, reqDup)

	if recDup.Code != http.StatusOK {
		t.Errorf("expected StatusOK for duplicate check, got %d", recDup.Code)
	}

	if len(producer.produced) != 1 {
		t.Errorf("expected event to NOT be produced twice")
	}

	invalidBody := `{"event_id":"id-999","event_type":"page_view","occurred_at":1718020000000,"payload":"{\"path\":\"/about\"}"}`
	reqInv := httptest.NewRequest("POST", "/v1/events", bytes.NewBufferString(invalidBody))
	recInv := httptest.NewRecorder()

	handler.ServeHTTP(recInv, reqInv)

	if recInv.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected StatusUnprocessableEntity, got %d", recInv.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	producer := &mockProducer{}
	deduper := &mockDeduper{}
	handler := v1.NewHandler(producer, deduper)

	req := httptest.NewRequest("GET", "/v1/events", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected StatusMethodNotAllowed, got %d", rec.Code)
	}
}

type failDeduper struct{}

func (f *failDeduper) SeenBefore(ctx context.Context, eventID string) (bool, error) {
	return false, errors.New("redis down")
}

func TestDeduperFailure(t *testing.T) {
	producer := &mockProducer{}
	deduper := &failDeduper{}
	handler := v1.NewHandler(producer, deduper)

	reqBody := `{"event_id":"id-123","event_type":"page_view","occurred_at":1718020000000,"payload":"{\"url\":\"http://test.com\"}"}`
	req := httptest.NewRequest("POST", "/v1/events", bytes.NewBufferString(reqBody))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected StatusServiceUnavailable, got %d", rec.Code)
	}
}
