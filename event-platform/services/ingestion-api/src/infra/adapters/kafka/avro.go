package kafka

import (
	"bytes"
	"encoding/binary"
	"sync"

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

var (
	rawEventSchema = avro.MustParse(rawEventSchemaJSON)
	bufferPool     = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

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

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	buf.WriteByte(0x0)
	var idBytes [4]byte
	binary.BigEndian.PutUint32(idBytes[:], schemaID)
	buf.Write(idBytes[:])
	buf.Write(encoded)

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res, nil
}
