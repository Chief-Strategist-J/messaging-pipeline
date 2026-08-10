package com.platform.streams.topology.processors

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.TransformerSupplier
import com.platform.streams.serde.RawEvent
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ProcessorRegistryTest {

    @Test
    fun `registered processor is retrievable`() {
        val supplier = TransformerSupplier<String, RawEvent, KeyValue<String, RawEvent>?> { null }
        ProcessorRegistry.register("testProc", supplier)
        val result = ProcessorRegistry.get("testProc")
        assertNotNull(result)
    }

    @Test
    fun `unregistered processor throws IllegalStateException`() {
        val ex = assertThrows<IllegalStateException> {
            ProcessorRegistry.get("missing_processor")
        }
        assertTrue(ex.message?.contains("missing_processor") == true)
    }
}
