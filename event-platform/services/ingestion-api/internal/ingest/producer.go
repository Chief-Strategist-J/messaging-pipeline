package ingest

import (
	"context"
	"log"

	"event-platform/ingestion-api/internal/constants"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type Producer interface {
	Produce(ctx context.Context, topic string, evt RawEvent) error
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

func (p *kafkaProducer) Produce(ctx context.Context, topic string, evt RawEvent) error {
	avroBytes, err := encodeAvro(evt, p.schemaID)
	if err != nil {
		log.Printf("avro encoding error: %v", err)
		return err
	}
	record := &kgo.Record{Topic: topic, Key: []byte(evt.EventID), Value: avroBytes}
	res := p.client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		log.Printf("kafka produce sync error: %v", err)
		return err
	}
	return nil
}

func (p *kafkaProducer) Close() { p.client.Close() }

type tracedProducer struct{ inner *kafkaProducer }

func (t *tracedProducer) Produce(ctx context.Context, topic string, evt RawEvent) error {
	_, span := otel.Tracer(constants.ServiceName).Start(ctx, constants.SpanKafkaProduce)
	span.SetAttributes(attribute.String(constants.AttrKafkaTopic, topic))
	defer span.End()
	return t.inner.Produce(ctx, topic, evt)
}
func (t *tracedProducer) Close() { t.inner.Close() }
