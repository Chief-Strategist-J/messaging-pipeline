package constants

import "time"

const (
	ServiceName = "ingestion-api"

	RouteEvents = "/v1/events"
	RouteHealth = "/healthz"

	SpanHTTPIngest  = "http.ingest"
	SpanKafkaProduce = "kafka.produce"

	AttrKafkaTopic = "kafka.topic"

	DedupKeyPrefix = "dedup:"
	DedupTTL       = 24 * time.Hour

	ProducerLingerMs = 8 * time.Millisecond

	CustomProcessorPurchase = "purchaseEnrichment"

	CurrencyField = "currency"

	ErrMethodNotAllowed   = "method not allowed"
	ErrInvalidPayload     = "invalid payload"
	ErrUnregisteredType   = "unregistered event_type"
	ErrProcessingFailed   = "processing failed: "
	ErrDedupCheckFailed   = "dedup check failed"
	ErrIngestFailed       = "ingest failed"
	ErrAtCapacity         = "at capacity, retry with backoff"
	ErrTraceExporterInit  = "failed to create trace exporter: "
	ErrResourceInit       = "failed to create resource: "
	ErrEventTypeMissing   = "event type config invalid, refusing to start: %v"
	ErrKafkaProducerInit  = "kafka producer init: %v"
	ErrServerError        = "server error: %v"
	ErrShutdownFailed     = "graceful shutdown failed: %v"

	LogListening = "listening on %s"
)
