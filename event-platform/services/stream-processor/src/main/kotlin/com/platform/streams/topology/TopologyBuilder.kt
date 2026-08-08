package com.platform.streams.topology

import com.platform.streams.Constants
import com.platform.streams.serde.AvroSerdes
import com.platform.streams.serde.RawEvent
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import java.time.Duration

class TopologyBuilder(
    private val definition: TopologyDefinition,
    private val serdes: AvroSerdes
) {
    fun build(): Topology {
        val builder = StreamsBuilder()
        var stream = builder.stream(
            definition.sourceTopic,
            Consumed.with(serdes.stringSerde(), serdes.rawEventSerde())
        )

        for (step in definition.steps) {
            stream = StepRegistry.get(step.type)(stream, step.config)
        }

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

private fun extractField(evt: RawEvent, field: String): String =
    when (field) {
        Constants.GROUP_BY_EVENT_TYPE -> evt.eventType
        else -> evt.eventType
    }
