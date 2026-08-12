package tracing

import (
	"context"
	"log"
	"os"

	"event-platform/ingestion-api/src/shared/constants"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type otelErrorHandler struct{}

func (h otelErrorHandler) Handle(err error) {
	log.Printf("[OTEL-ERROR] %v", err)
}

func InitTracing(otlpEndpoint string) func(context.Context) {
	otel.SetErrorHandler(otelErrorHandler{})
	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(otlpEndpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		panic(constants.ErrTraceExporterInit + err.Error())
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "ingestion-api-instance"
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(constants.ServiceName),
		semconv.ServiceInstanceID(hostname),
	))

	bsp := trace.NewBatchSpanProcessor(exporter)

	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithSpanProcessor(bsp),
		trace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return func(ctx context.Context) {
		_ = tp.Shutdown(ctx)
	}
}
