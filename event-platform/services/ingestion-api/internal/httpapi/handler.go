package httpapi

import (
	"encoding/json"
	"io"
	"net/http"

	"event-platform/ingestion-api/internal/constants"
	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/ingest"
	"github.com/buger/jsonparser"
)

type Handler struct {
	producer ingest.Producer
	deduper  ingest.Deduper
}

func NewHandler(p ingest.Producer, d ingest.Deduper) *Handler {
	return &Handler{producer: p, deduper: d}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) == 0 {
		http.Error(w, constants.ErrInvalidPayload, http.StatusBadRequest)
		return
	}

	eventID, err1 := jsonparser.GetString(body, "event_id")
	eventType, err2 := jsonparser.GetString(body, "event_type")
	payloadBytes, _, _, err3 := jsonparser.Get(body, "payload")

	if err1 != nil || err2 != nil || err3 != nil {
		if !json.Valid(body) {
			http.Error(w, constants.ErrInvalidPayload, http.StatusBadRequest)
			return
		}
	}

	occurredAt, _ := jsonparser.GetInt(body, "occurred_at")

	evt := ingest.RawEvent{
		EventID:    eventID,
		EventType:  eventType,
		OccurredAt: occurredAt,
		Payload:    json.RawMessage(payloadBytes),
	}

	if err := evt.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	cfg, ok := eventtypes.Get(evt.EventType)
	if !ok {
		http.Error(w, constants.ErrUnregisteredType, http.StatusUnprocessableEntity)
		return
	}

	if err := eventtypes.ValidatePayload(cfg, string(evt.Payload)); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	if proc, ok := eventtypes.GetCustomProcessor(cfg.CustomProcessor); ok {
		enriched, err := proc(string(evt.Payload))
		if err != nil {
			http.Error(w, constants.ErrProcessingFailed+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		evt.Payload = json.RawMessage(enriched)
	}

	seen, err := h.deduper.SeenBefore(r.Context(), evt.EventID)
	if err != nil {
		http.Error(w, constants.ErrDedupCheckFailed, http.StatusServiceUnavailable)
		return
	}
	if seen {
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := h.producer.Produce(r.Context(), cfg.Topic, evt); err != nil {
		http.Error(w, constants.ErrIngestFailed, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
