package com.platform.streams.serde

import com.platform.streams.Constants
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

data class RawEvent(
    val eventId: String,
    val eventType: String,
    val occurredAt: Long,
    val payload: String
)

class RawEventSerializer : Serializer<RawEvent> {
    override fun serialize(topic: String?, data: RawEvent?): ByteArray? {
        if (data == null) return null
        val eventIdBytes = data.eventId.toByteArray(StandardCharsets.UTF_8)
        val eventTypeBytes = data.eventType.toByteArray(StandardCharsets.UTF_8)
        val payloadBytes = data.payload.toByteArray(StandardCharsets.UTF_8)

        val totalSize = 4 + eventIdBytes.size +
                        4 + eventTypeBytes.size +
                        8 +
                        4 + payloadBytes.size

        val buffer = ByteBuffer.allocate(totalSize)
        buffer.putInt(eventIdBytes.size)
        buffer.put(eventIdBytes)
        buffer.putInt(eventTypeBytes.size)
        buffer.put(eventTypeBytes)
        buffer.putLong(data.occurredAt)
        buffer.putInt(payloadBytes.size)
        buffer.put(payloadBytes)

        return buffer.array()
    }
}

class RawEventDeserializer : Deserializer<RawEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): RawEvent? {
        if (data == null || data.size < 16) return null
        val buffer = ByteBuffer.wrap(data)

        val eventIdLen = buffer.int
        if (buffer.remaining() < eventIdLen) return null
        val eventIdBytes = ByteArray(eventIdLen)
        buffer.get(eventIdBytes)

        val eventTypeLen = buffer.int
        if (buffer.remaining() < eventTypeLen) return null
        val eventTypeBytes = ByteArray(eventTypeLen)
        buffer.get(eventTypeBytes)

        val occurredAt = buffer.long

        val payloadLen = buffer.int
        if (buffer.remaining() < payloadLen) return null
        val payloadBytes = ByteArray(payloadLen)
        buffer.get(payloadBytes)

        return RawEvent(
            eventId = String(eventIdBytes, StandardCharsets.UTF_8),
            eventType = String(eventTypeBytes, StandardCharsets.UTF_8),
            occurredAt = occurredAt,
            payload = String(payloadBytes, StandardCharsets.UTF_8)
        )
    }
}

class AvroSerdes(private val schemaRegistryUrl: String) {
    private val config = mapOf(Constants.CONFIG_KEY_SCHEMA_REGISTRY_URL to schemaRegistryUrl)

    fun stringSerde() = Serdes.String()
    fun longSerde() = Serdes.Long()

    fun genericAvroSerde(): GenericAvroSerde =
        GenericAvroSerde().apply { configure(config, false) }

    fun rawEventSerde() = Serdes.serdeFrom(RawEventSerializer(), RawEventDeserializer())

    fun groupedByType(): Grouped<String, RawEvent> = Grouped.with(stringSerde(), rawEventSerde())
}
