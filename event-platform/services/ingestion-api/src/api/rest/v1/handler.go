package v1

import (
	"io"
	"net/http"

	"event-platform/ingestion-api/src/core/rules"
	"event-platform/ingestion-api/src/features/events"
	"event-platform/ingestion-api/src/infra/adapters/kafka"
	"event-platform/ingestion-api/src/infra/adapters/redis"
	"event-platform/ingestion-api/src/shared/constants"
)

type Handler struct {
	pipeline *rules.Engine
}

func NewHandler(p kafka.Producer, d redis.Deduper) *Handler {
	pipeline := events.FeatureCreateIngestionPipeline(p, d)
	return &Handler{pipeline: pipeline}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, constants.MaxBodyBytes)

	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) == 0 {
		http.Error(w, constants.ErrInvalidPayload, http.StatusBadRequest)
		return
	}

	evalCtx := rules.NewEvaluationContext(body)
	defer rules.PutEvaluationContext(evalCtx)

	_ = h.pipeline.Evaluate(r.Context(), evalCtx)

	switch evalCtx.ResultCode {
	case rules.ResultSuccess:
		w.WriteHeader(http.StatusAccepted)
	case rules.ResultDuplicate:
		w.WriteHeader(http.StatusOK)

	case rules.ResultInvalidPayload, rules.ResultUnregisteredType:
		errMsg := constants.ErrInvalidPayload
		if evalCtx.Err != nil {
			errMsg = evalCtx.Err.Error()
		}
		http.Error(w, errMsg, http.StatusUnprocessableEntity)
	case rules.ResultDedupCheckFailed, rules.ResultIngestFailed:
		errMsg := constants.ErrIngestFailed
		if evalCtx.ResultCode == rules.ResultDedupCheckFailed {
			errMsg = constants.ErrDedupCheckFailed
		}
		http.Error(w, errMsg, http.StatusServiceUnavailable)
	default:
		http.Error(w, constants.ErrInvalidPayload, http.StatusBadRequest)
	}
}
