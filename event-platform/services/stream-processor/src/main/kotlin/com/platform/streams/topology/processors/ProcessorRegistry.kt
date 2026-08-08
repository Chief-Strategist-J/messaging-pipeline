package com.platform.streams.topology.processors

import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.KeyValue

object ProcessorRegistry {
    private val registry = mutableMapOf<String, TransformerSupplier<String, Any, KeyValue<String, Any>>>()

    fun register(name: String, supplier: TransformerSupplier<String, Any, KeyValue<String, Any>>) {
        registry[name] = supplier
    }

    fun get(name: String): TransformerSupplier<String, Any, KeyValue<String, Any>> =
        registry[name] ?: throw IllegalStateException("No processor registered for \"$name\"")
}
