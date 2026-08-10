package kafka

import (
	"bytes"
	"encoding/binary"

	"github.com/hamba/avro/v2"
)

const rawEventSchemaJSON = `{
	"type": "record",
	"name": "RawEvent",
	"namespace": "com.platform.events",
	"fields": [
		{"name": "event_id", "type": "string"},
		{"name": "event_type", "type": "string"},
		{"name": "occurred_at", "type": "long", "logicalType": "timestamp-millis"},
		{"name": "payload", "type": "string"}
	]
}`

var rawEventSchema = avro.MustParse(rawEventSchemaJSON)

type avroRawEvent struct {
	EventID    string `avro:"event_id"`
	EventType  string `avro:"event_type"`
	OccurredAt int64  `avro:"occurred_at"`
	Payload    string `avro:"payload"`
}

func encodeAvro(eventID, eventType string, occurredAt int64, payload []byte, schemaID uint32) ([]byte, error) {
	aEvt := avroRawEvent{
		EventID:    eventID,
		EventType:  eventType,
		OccurredAt: occurredAt,
		Payload:    string(payload),
	}
	encoded, err := avro.Marshal(rawEventSchema, aEvt)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(0x0)
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, schemaID)
	buf.Write(idBytes)
	buf.Write(encoded)
	return buf.Bytes(), nil
}
