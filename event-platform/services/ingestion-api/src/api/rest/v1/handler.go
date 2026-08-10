package v1

import (
	"encoding/json"
	"io"
	"net/http"

	"event-platform/ingestion-api/src/features/events"
	"event-platform/ingestion-api/src/infra/adapters/kafka"
	"event-platform/ingestion-api/src/infra/adapters/redis"
	"event-platform/ingestion-api/src/shared/constants"
	"github.com/buger/jsonparser"
)

type Handler struct {
	producer kafka.Producer
	deduper  redis.Deduper
}

func NewHandler(p kafka.Producer, d redis.Deduper) *Handler {
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
	
	var payloadBytes []byte
	var err3 error
	rawVal, dataType, _, errGet := jsonparser.Get(body, "payload")
	if errGet != nil {
		err3 = errGet
	} else {
		if dataType == jsonparser.String {
			if strVal, errParse := jsonparser.ParseString(rawVal); errParse == nil {
				payloadBytes = []byte(strVal)
			} else {
				payloadBytes = rawVal
			}
		} else {
			payloadBytes = rawVal
		}
	}

	if err1 != nil || err2 != nil || err3 != nil {
		if !json.Valid(body) {
			http.Error(w, constants.ErrInvalidPayload, http.StatusBadRequest)
			return
		}
	}

	occurredAt, _ := jsonparser.GetInt(body, "occurred_at")

	evt := events.RawEvent{
		EventID:    eventID,
		EventType:  eventType,
		OccurredAt: occurredAt,
		Payload:    string(payloadBytes),
	}

	if err := evt.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	cfg, ok := events.FeatureGet(evt.EventType)
	if !ok {
		http.Error(w, constants.ErrUnregisteredType, http.StatusUnprocessableEntity)
		return
	}

	if err := events.FeatureValidatePayload(cfg, payloadBytes); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	if proc, ok := events.FeatureGetCustomProcessor(cfg.CustomProcessor); ok {
		enriched, err := proc(payloadBytes)
		if err != nil {
			http.Error(w, constants.ErrProcessingFailed+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		evt.Payload = string(enriched)
		payloadBytes = enriched
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

	if err := h.producer.Produce(r.Context(), cfg.Topic, evt.EventID, evt.EventType, evt.OccurredAt, payloadBytes); err != nil {
		http.Error(w, constants.ErrIngestFailed, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
