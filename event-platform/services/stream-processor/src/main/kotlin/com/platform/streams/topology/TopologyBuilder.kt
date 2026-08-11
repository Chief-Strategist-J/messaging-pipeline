package com.platform.streams.topology

import com.platform.streams.serde.AvroSerdes
import com.platform.streams.serde.RawEvent
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.common.serialization.Serdes
import org.apache.avro.generic.GenericRecord
import java.time.Duration

class TopologyBuilder(
    private val definition: TopologyDefinition,
    private val serdes: AvroSerdes
) {
    fun build(): Topology {
        val builder = StreamsBuilder()

        val dedupStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("dedup-store"),
            Serdes.String(),
            Serdes.Long()
        )
        builder.addStateStore(dedupStoreBuilder)

        val stream = builder.stream(
            definition.sourceTopic,
            Consumed.with(serdes.stringSerde(), serdes.genericAvroSerde())
        )

        var rawStream = stream.mapValues { _, v ->
            val tracer = io.opentelemetry.api.GlobalOpenTelemetry.getTracer("stream-processor")
            val span = tracer.spanBuilder("kotlin-stream:map-raw-event").startSpan()
            try {
                RawEvent(
                    eventId = v.get("event_id")?.toString() ?: "",
                    eventType = v.get("event_type")?.toString() ?: "",
                    occurredAt = v.get("occurred_at") as? Long ?: 0L,
                    payload = v.get("payload")?.toString() ?: ""
                )
            } finally {
                span.end()
            }
        }

        for (step in definition.steps) {
            rawStream = StepRegistry.get(step.type)(rawStream, step.config)
        }

        rawStream
            .groupBy({ _, v -> extractField(v, definition.groupByField) }, serdes.groupedByType())
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(definition.windowMinutes)))
            .count()
            .toStream()
            .map { windowedKey, count -> org.apache.kafka.streams.KeyValue(windowedKey.key(), count) }
            .to(definition.sinkTopic, Produced.with(serdes.stringSerde(), serdes.longSerde()))

        return builder.build()
    }
}

private fun extractField(evt: RawEvent, field: String): String =
    if (field == "eventType") evt.eventType else evt.eventType
