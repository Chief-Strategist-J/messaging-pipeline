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

	carrier := &RecordHeadersCarrier{Headers: record.Headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	record.Headers = carrier.Headers

	errChan := make(chan error, 1)
	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		errChan <- err
	})

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
