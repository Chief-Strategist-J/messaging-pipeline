package com.platform.streams.topology.processors

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.WindowStore
import com.platform.streams.serde.RawEvent

class DedupTransformer(private val retentionMs: Long = 24 * 3600 * 1000L) : Transformer<String, RawEvent, KeyValue<String, RawEvent>?> {
    private lateinit var store: WindowStore<String, Long>

    override fun init(context: ProcessorContext) {
        @Suppress("UNCHECKED_CAST")
        this.store = context.getStateStore("dedup-store") as WindowStore<String, Long>
    }

    override fun transform(key: String, value: RawEvent?): KeyValue<String, RawEvent>? {
        if (value == null) return null
        val eventId = value.eventId
        val eventTime = value.occurredAt
        val timeFrom = eventTime - retentionMs
        
        val iter = store.fetch(eventId, timeFrom, eventTime)
        val isDuplicate = iter.hasNext()
        iter.close()

        if (isDuplicate) {
            return null
        }

        store.put(eventId, eventTime, eventTime)
        return KeyValue(key, value)
    }

    override fun close() {}
}
