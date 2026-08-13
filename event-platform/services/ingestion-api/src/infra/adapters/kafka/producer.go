package kafka

import (
	"context"
	"log/slog"

	"event-platform/ingestion-api/src/shared/constants"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Producer interface {
	Produce(ctx context.Context, topic string, eventID string, eventType string, occurredAt int64, payload []byte) error
	Close()
}

type kafkaProducer struct {
	client   *kgo.Client
	schemaID uint32
}

func NewKafkaProducer(brokers []string, schemaID uint32) (Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(constants.ProducerLingerMs),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &tracedProducer{inner: &kafkaProducer{client: client, schemaID: schemaID}}, nil
}

func (p *kafkaProducer) Close() {
	p.client.Flush(context.Background())
	p.client.Close()
}

type tracedProducer struct {
	inner *kafkaProducer
}

func (t *tracedProducer) Produce(ctx context.Context, topic string, eventID string, eventType string, occurredAt int64, payload []byte) error {
	_, span := otel.Tracer(constants.ServiceName).Start(ctx, constants.SpanKafkaProduce)
	span.SetAttributes(
		attribute.String(constants.AttrKafkaTopic, topic),
		attribute.String("messaging.destination.name", topic),
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.message.id", eventID),
	)

	spanCtx := trace.ContextWithSpan(ctx, span)

	avroBytes, encErr := encodeAvro(eventID, eventType, occurredAt, payload, t.inner.schemaID)
	if encErr != nil {
		span.SetStatus(codes.Error, encErr.Error())
		span.End()
		return encErr
	}

	record := &kgo.Record{Topic: topic, Key: []byte(eventID), Value: avroBytes}
	carrier := &RecordHeadersCarrier{Headers: &record.Headers}
	otel.GetTextMapPropagator().Inject(spanCtx, carrier)

	t.inner.client.Produce(context.Background(), record, func(_ *kgo.Record, deliveryErr error) {
		if deliveryErr != nil {
			span.SetStatus(codes.Error, deliveryErr.Error())
			slog.Error("kafka delivery failed",
				"topic", topic,
				"event_id", eventID,
				"error", deliveryErr,
			)
		}
		span.End()
	})

	return nil
}

func (t *tracedProducer) Close() {
	t.inner.Close()
}

type RecordHeadersCarrier struct {
	Headers *[]kgo.RecordHeader
}

func (c *RecordHeadersCarrier) Get(key string) string {
	if c.Headers == nil {
		return ""
	}
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *RecordHeadersCarrier) Set(key string, value string) {
	if c.Headers == nil {
		return
	}
	valBytes := []byte(value)
	headers := *c.Headers
	for i := range headers {
		if headers[i].Key == key {
			headers[i].Value = valBytes
			return
		}
	}
	*c.Headers = append(headers, kgo.RecordHeader{
		Key:   key,
		Value: valBytes,
	})
}

var emptyCarrierKeys = []string{"traceparent", "tracestate", "baggage"}

func (c *RecordHeadersCarrier) Keys() []string {
	return emptyCarrierKeys
}
