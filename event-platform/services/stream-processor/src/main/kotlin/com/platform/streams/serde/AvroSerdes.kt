package com.platform.streams.serde

import com.platform.streams.Constants
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.avro.generic.GenericRecord
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
        // Simple and robust text serialization to avoid heavy JSON parsing dependencies
        val str = "${data.eventId}|${data.eventType}|${data.occurredAt}|${data.payload}"
        return str.toByteArray(StandardCharsets.UTF_8)
    }
}

class RawEventDeserializer : Deserializer<RawEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): RawEvent? {
        if (data == null) return null
        val str = String(data, StandardCharsets.UTF_8)
        val parts = str.split("|", limit = 4)
        if (parts.size < 4) return null
        return RawEvent(
            eventId = parts[0],
            eventType = parts[1],
            occurredAt = parts[2].toLongOrNull() ?: 0L,
            payload = parts[3]
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
