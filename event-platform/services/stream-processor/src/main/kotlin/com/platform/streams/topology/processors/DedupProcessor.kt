package com.platform.streams.topology.processors

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import com.platform.streams.serde.RawEvent

class DedupTransformer(private val retentionMs: Long = 24 * 3600 * 1000L) : Transformer<String, RawEvent, KeyValue<String, RawEvent>?> {
    private lateinit var store: KeyValueStore<String, Long>
    private val tracer = io.opentelemetry.api.GlobalOpenTelemetry.getTracer("stream-processor")

    override fun init(context: ProcessorContext) {
        @Suppress("UNCHECKED_CAST")
        this.store = context.getStateStore("dedup-store") as KeyValueStore<String, Long>
    }

    override fun transform(key: String, value: RawEvent?): KeyValue<String, RawEvent>? {
        val span = tracer.spanBuilder("kotlin-stream:dedup-check").startSpan()
        try {
            if (value == null) return null
            val eventId = value.eventId
            val prevOccurredAt = store.get(eventId)
            if (prevOccurredAt != null) {
                if (value.occurredAt - prevOccurredAt < retentionMs) {
                    return null
                }
            }
            store.put(eventId, value.occurredAt)
            return KeyValue(key, value)
        } finally {
            span.end()
        }
    }

    override fun close() {}
}
