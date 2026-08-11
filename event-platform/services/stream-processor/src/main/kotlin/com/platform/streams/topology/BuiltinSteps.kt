package com.platform.streams.topology

import com.platform.streams.topology.processors.ProcessorRegistry
import com.platform.streams.topology.processors.DedupTransformer
import org.apache.kafka.streams.kstream.TransformerSupplier

fun registerBuiltinSteps() {
    ProcessorRegistry.register("dedup", TransformerSupplier { DedupTransformer() })

    StepRegistry.register("dedup") { stream, _ ->
        stream.transform(ProcessorRegistry.get("dedup"), "dedup-store")
    }

    StepRegistry.register("filterByType") { stream, config ->
        @Suppress("UNCHECKED_CAST")
        val allowed = config["allowedTypes"] as? List<String> ?: emptyList()
        stream.filter { _, v -> allowed.isEmpty() || v.eventType in allowed }
    }
}
