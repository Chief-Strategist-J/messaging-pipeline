package com.platform.streams.topology

import com.platform.streams.Constants
import com.platform.streams.topology.processors.ProcessorRegistry

fun registerBuiltinSteps() {
    StepRegistry.register(Constants.STEP_DEDUP) { stream, _ ->
        stream.transform(ProcessorRegistry.get(Constants.STEP_DEDUP))
    }

    StepRegistry.register(Constants.STEP_FILTER_BY_TYPE) { stream, config ->
        @Suppress("UNCHECKED_CAST")
        val allowed = config[Constants.CONFIG_KEY_ALLOWED_TYPES] as? List<String> ?: emptyList()
        stream.filter { _, v -> allowed.isEmpty() || v.eventType in allowed }
    }
}
