package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/httpapi"
)

type mockProducer struct {
	produced []producedRecord
	failNext bool
}

type producedRecord struct {
	topic string
	id    string
}

func (m *mockProducer) Produce(_ context.Context, topic string, evt interface{ GetID() string }) error {
	if m.failNext {
		return errProduceFailed
	}
	m.produced = append(m.produced, producedRecord{topic: topic})
	return nil
}

func (m *mockProducer) Close() {}

type mockDeduper struct {
	seen     map[string]bool
	failNext bool
}

func newMockDeduper() *mockDeduper {
	return &mockDeduper{seen: map[string]bool{}}
}

func (d *mockDeduper) SeenBefore(_ context.Context, eventID string) (bool, error) {
	if d.failNext {
		return false, errDedupFailed
	}
	if d.seen[eventID] {
		return true, nil
	}
	d.seen[eventID] = true
	return false, nil
}

var (
	errProduceFailed = errorString("produce failed")
	errDedupFailed   = errorString("dedup failed")
)

type errorString string

func (e errorString) Error() string { return string(e) }

func setupTestHandler(t *testing.T) (*httpapi.Handler, *mockDeduper) {
	t.Helper()
	_ = eventtypes.LoadFromConfig(eventtypes.Config{
		EventTypes: []eventtypes.EventTypeConfig{
			{Name: "page_view", Topic: "events.raw", PayloadRules: []eventtypes.FieldRule{{Field: "url", Required: true}}},
			{Name: "heartbeat", Topic: "events.raw"},
		},
	})
	deduper := newMockDeduper()
	handler := httpapi.NewHandler(nil, deduper)
	return handler, deduper
}

func TestHandlerRejectsGetMethod(t *testing.T) {
	handler, _ := setupTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestHandlerRejectsInvalidJSON(t *testing.T) {
	handler, _ := setupTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader("{broken"))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestHandlerRejectsMissingEventID(t *testing.T) {
	handler, _ := setupTestHandler(t)
	body := `{"event_type":"page_view","payload":"{\"url\":\"test\"}"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d", rec.Code)
	}
}

func TestHandlerRejectsUnregisteredEventType(t *testing.T) {
	handler, _ := setupTestHandler(t)
	body := `{"event_id":"1","event_type":"unknown_type","payload":"{}"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d", rec.Code)
	}
}

func TestHandlerRejectsInvalidPayload(t *testing.T) {
	handler, _ := setupTestHandler(t)
	body := `{"event_id":"1","event_type":"page_view","payload":"{\"not_url\":\"x\"}"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d", rec.Code)
	}
}

func TestHandlerReturnOKForDuplicate(t *testing.T) {
	handler, deduper := setupTestHandler(t)
	deduper.seen["dup-id"] = true

	body := `{"event_id":"dup-id","event_type":"heartbeat","payload":"{}"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for duplicate, got %d", rec.Code)
	}
}
