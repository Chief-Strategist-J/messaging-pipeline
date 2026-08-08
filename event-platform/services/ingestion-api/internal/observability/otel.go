package observability

import (
	"context"

	"event-platform/ingestion-api/internal/constants"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func InitTracing(otlpEndpoint string) func(context.Context) {
	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(otlpEndpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		panic(constants.ErrTraceExporterInit + err.Error())
	}

	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(constants.ServiceName)))
	if err != nil {
		panic(constants.ErrResourceInit + err.Error())
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return func(ctx context.Context) {
		_ = tp.Shutdown(ctx)
	}
}
