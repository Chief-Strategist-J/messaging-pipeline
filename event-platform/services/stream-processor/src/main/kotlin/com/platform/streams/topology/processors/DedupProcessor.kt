package com.platform.streams.topology.processors

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.KeyValueStore
import com.platform.streams.serde.RawEvent
import java.time.Duration

class DedupTransformer(private val retentionMs: Long = 24 * 3600 * 1000L) : Transformer<String, RawEvent, KeyValue<String, RawEvent>?> {
    private lateinit var store: KeyValueStore<String, Long>

    override fun init(context: ProcessorContext) {
        @Suppress("UNCHECKED_CAST")
        this.store = context.getStateStore("dedup-store") as KeyValueStore<String, Long>

        // Periodically evict expired event IDs from RocksDB to prevent unbounded state growth
        context.schedule(Duration.ofMinutes(10), PunctuationType.WALL_CLOCK_TIME) { timestamp ->
            val iter = store.all()
            while (iter.hasNext()) {
                val entry = iter.next()
                if (timestamp - entry.value > retentionMs) {
                    store.delete(entry.key)
                }
            }
            iter.close()
        }
    }

    override fun transform(key: String, value: RawEvent?): KeyValue<String, RawEvent>? {
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
    }

    override fun close() {}
}
