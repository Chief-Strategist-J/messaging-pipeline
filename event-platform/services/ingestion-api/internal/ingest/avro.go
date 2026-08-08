package ingest

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
		{"name": "occurred_at", "type": "long"},
		{"name": "payload", "type": "string"}
	]
}`

var rawEventSchema = avro.MustParse(rawEventSchemaJSON)

func encodeAvro(evt RawEvent, schemaID uint32) ([]byte, error) {
	payload, err := avro.Marshal(rawEventSchema, evt)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(0x0)
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, schemaID)
	buf.Write(idBytes)
	buf.Write(payload)
	return buf.Bytes(), nil
}
