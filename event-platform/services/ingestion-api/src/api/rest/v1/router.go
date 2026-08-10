package v1

import (
	"net/http"

	"event-platform/ingestion-api/src/shared/constants"
)

func NewRouter(handler *Handler, maxConcurrent int) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(constants.RouteEvents, WithTracing(WithRateLimit(handler, maxConcurrent)))
	mux.HandleFunc(constants.RouteHealth, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
