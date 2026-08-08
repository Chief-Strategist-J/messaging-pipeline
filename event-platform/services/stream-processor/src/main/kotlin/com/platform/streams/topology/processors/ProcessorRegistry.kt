package com.platform.streams.topology.processors

import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.KeyValue
import com.platform.streams.serde.RawEvent

object ProcessorRegistry {
    private val registry = mutableMapOf<String, TransformerSupplier<String, RawEvent, KeyValue<String, RawEvent>>>()

    fun register(name: String, supplier: TransformerSupplier<String, RawEvent, KeyValue<String, RawEvent>>) {
        registry[name] = supplier
    }

    fun get(name: String): TransformerSupplier<String, RawEvent, KeyValue<String, RawEvent>> =
        registry[name] ?: throw IllegalStateException("No processor registered for \"$name\"")
}
