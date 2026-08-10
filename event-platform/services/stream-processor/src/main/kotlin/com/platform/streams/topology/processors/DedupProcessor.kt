package com.platform.streams.topology.processors

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import com.platform.streams.serde.RawEvent

class DedupTransformer : Transformer<String, RawEvent, KeyValue<String, RawEvent>?> {
    private lateinit var store: KeyValueStore<String, Long>

    override fun init(context: ProcessorContext) {
        @Suppress("UNCHECKED_CAST")
        this.store = context.getStateStore("dedup-store") as KeyValueStore<String, Long>
    }

    override fun transform(key: String, value: RawEvent?): KeyValue<String, RawEvent>? {
        if (value == null) return null
        val eventId = value.eventId
        if (store.get(eventId) != null) {
            return null // Duplicate detected, drop it
        }
        store.put(eventId, value.occurredAt)
        return KeyValue(key, value)
    }

    override fun close() {}
}
