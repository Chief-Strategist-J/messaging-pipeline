package kafka

import (
	"context"

	"event-platform/ingestion-api/src/shared/constants"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

func (p *kafkaProducer) Produce(ctx context.Context, topic string, eventID string, eventType string, occurredAt int64, payload []byte) error {
	avroBytes, err := encodeAvro(eventID, eventType, occurredAt, payload, p.schemaID)
	if err != nil {
		return err
	}
	record := &kgo.Record{Topic: topic, Key: []byte(eventID), Value: avroBytes}

	// Propagate OpenTelemetry tracing context via Kafka Record Headers
	carrier := &RecordHeadersCarrier{Headers: record.Headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	record.Headers = carrier.Headers

	res := p.client.ProduceSync(ctx, record)
	return res.FirstErr()
}

func (p *kafkaProducer) Close() {
	p.client.Close()
}

type tracedProducer struct {
	inner *kafkaProducer
}

func (t *tracedProducer) Produce(ctx context.Context, topic string, eventID string, eventType string, occurredAt int64, payload []byte) error {
	ctx, span := otel.Tracer(constants.ServiceName).Start(ctx, constants.SpanKafkaProduce)
	span.SetAttributes(attribute.String(constants.AttrKafkaTopic, topic))
	defer span.End()
	return t.inner.Produce(ctx, topic, eventID, eventType, occurredAt, payload)
}

func (t *tracedProducer) Close() {
	t.inner.Close()
}

// RecordHeadersCarrier implements propagation.TextMapCarrier for Kafka headers
type RecordHeadersCarrier struct {
	Headers []kgo.RecordHeader
}

func (c *RecordHeadersCarrier) Get(key string) string {
	for _, h := range c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *RecordHeadersCarrier) Set(key string, value string) {
	for i, h := range c.Headers {
		if h.Key == key {
			c.Headers[i].Value = []byte(value)
			return
		}
	}
	c.Headers = append(c.Headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
}

func (c *RecordHeadersCarrier) Keys() []string {
	keys := make([]string, len(c.Headers))
	for i, h := range c.Headers {
		keys[i] = h.Key
	}
	return keys
}
