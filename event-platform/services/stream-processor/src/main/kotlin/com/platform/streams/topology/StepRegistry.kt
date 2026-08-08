package com.platform.streams.topology

import org.apache.kafka.streams.kstream.KStream
import com.platform.streams.serde.RawEvent

typealias StreamStepFn = (KStream<String, RawEvent>, Map<String, Any>) -> KStream<String, RawEvent>

object StepRegistry {
    private val steps = mutableMapOf<String, StreamStepFn>()

    fun register(type: String, fn: StreamStepFn) {
        steps[type] = fn
    }

    fun get(type: String): StreamStepFn =
        steps[type] ?: throw IllegalStateException("No topology step registered for type \"$type\"")
}
