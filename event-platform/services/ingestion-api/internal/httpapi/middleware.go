package httpapi

import (
	"net/http"

	"event-platform/ingestion-api/internal/constants"
	"go.opentelemetry.io/otel"
)

func WithTracing(next http.Handler) http.Handler {
	tracer := otel.Tracer(constants.ServiceName)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), constants.SpanHTTPIngest)
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func WithRateLimit(next http.Handler, maxConcurrent int) http.Handler {
	sem := make(chan struct{}, maxConcurrent)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			next.ServeHTTP(w, r)
		default:
			http.Error(w, constants.ErrAtCapacity, http.StatusServiceUnavailable)
		}
	})
}
