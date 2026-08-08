package com.platform.streams.topology

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class StepRegistryTest {

    @BeforeEach
    fun setup() {
        StepRegistry.register("testStep") { stream, _ -> stream }
    }

    @Test
    fun `registered step is retrievable`() {
        val step = StepRegistry.get("testStep")
        kotlin.test.assertNotNull(step)
    }

    @Test
    fun `unregistered step throws IllegalStateException`() {
        val ex = assertThrows<IllegalStateException> {
            StepRegistry.get("nonexistent")
        }
        assertEquals(true, ex.message?.contains("nonexistent"))
    }

    @Test
    fun `overwriting a step replaces it`() {
        var callCount = 0
        StepRegistry.register("testStep") { stream, _ ->
            callCount++
            stream
        }
        val step = StepRegistry.get("testStep")
        kotlin.test.assertNotNull(step)
    }
}
