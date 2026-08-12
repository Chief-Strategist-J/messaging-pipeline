package v1

import (
	"log"
	"net/http"

	"event-platform/ingestion-api/src/shared/constants"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func WithTracing(next http.Handler) http.Handler {
	propagator := otel.GetTextMapPropagator()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		tracer := otel.GetTracerProvider().Tracer(constants.ServiceName)
		spanName := r.Method + " " + r.URL.Path
		ctx, span := tracer.Start(ctx, spanName)
		defer span.End()
		if span.SpanContext().IsValid() {
			w.Header().Set("X-Trace-Id", span.SpanContext().TraceID().String())
			log.Printf("[SPAN-RECORDED] trace_id=%s span_id=%s sampled=%v recording=%v",
				span.SpanContext().TraceID().String(),
				span.SpanContext().SpanID().String(),
				span.SpanContext().IsSampled(),
				span.IsRecording(),
			)
		}
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
