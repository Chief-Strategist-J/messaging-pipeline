package com.platform.streams.topology

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertNotNull

class BuiltinStepsTest {

    @BeforeEach
    fun setup() {
        registerBuiltinSteps()
    }

    @Test
    fun `dedup step is registered`() {
        val step = StepRegistry.get("dedup")
        assertNotNull(step)
    }

    @Test
    fun `filterByType step is registered`() {
        val step = StepRegistry.get("filterByType")
        assertNotNull(step)
    }
}
