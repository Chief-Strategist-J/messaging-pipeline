package httpapi

import (
	"encoding/json"
	"net/http"

	"event-platform/ingestion-api/internal/constants"
	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/ingest"
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

	var evt ingest.RawEvent
	if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
		http.Error(w, constants.ErrInvalidPayload, http.StatusBadRequest)
		return
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

	if err := eventtypes.ValidatePayload(cfg, evt.Payload); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	if proc, ok := eventtypes.GetCustomProcessor(cfg.CustomProcessor); ok {
		enriched, err := proc(evt.Payload)
		if err != nil {
			http.Error(w, constants.ErrProcessingFailed+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		evt.Payload = enriched
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
