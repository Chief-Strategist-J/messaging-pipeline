package com.platform.streams.topology

import com.platform.streams.Constants
import com.platform.streams.serde.AvroSerdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.avro.generic.GenericRecord
import java.time.Duration

class TopologyBuilder(
    private val definition: TopologyDefinition,
    private val serdes: AvroSerdes
) {
    fun build(): Topology {
        val builder = StreamsBuilder()
        val stream = builder.stream(
            definition.sourceTopic,
            Consumed.with(serdes.stringSerde(), serdes.genericAvroSerde())
        )

        stream
            .groupBy({ _, v -> extractField(v, definition.groupByField) }, serdes.groupedByType())
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(definition.windowMinutes)))
            .count()
            .toStream()
            .map { windowedKey, count -> KeyValue(windowedKey.key(), count) }
            .to(definition.sinkTopic, Produced.with(serdes.stringSerde(), serdes.longSerde()))

        return builder.build()
    }
}

private fun extractField(evt: GenericRecord, field: String): String =
    evt.get(field)?.toString() ?: ""
