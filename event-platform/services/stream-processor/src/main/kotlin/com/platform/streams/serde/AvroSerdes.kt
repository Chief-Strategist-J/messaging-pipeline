package com.platform.streams.serde

import com.platform.streams.Constants
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Grouped
import org.apache.avro.generic.GenericRecord

data class RawEvent(
    val eventId: String,
    val eventType: String,
    val occurredAt: Long,
    val payload: String
)

class AvroSerdes(private val schemaRegistryUrl: String) {
    private val config = mapOf(Constants.CONFIG_KEY_SCHEMA_REGISTRY_URL to schemaRegistryUrl)

    fun stringSerde() = Serdes.String()
    fun longSerde() = Serdes.Long()

    fun genericAvroSerde(): GenericAvroSerde =
        GenericAvroSerde().apply { configure(config, false) }

    fun groupedByType(): Grouped<String, GenericRecord> = Grouped.with(stringSerde(), genericAvroSerde())
}
