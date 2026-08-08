package com.platform.streams.topology

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TopologyDefinitionTest {

    @Test
    fun `default values are set correctly`() {
        val def = TopologyDefinition(
            sourceTopic = "events.raw",
            steps = emptyList(),
            sinkTopic = "events.enriched"
        )
        assertEquals("eventType", def.groupByField)
        assertEquals(1L, def.windowMinutes)
    }

    @Test
    fun `custom values override defaults`() {
        val def = TopologyDefinition(
            sourceTopic = "input",
            steps = listOf(TopologyStep(id = "s1", type = "dedup")),
            groupByField = "region",
            windowMinutes = 5,
            sinkTopic = "output"
        )
        assertEquals("input", def.sourceTopic)
        assertEquals("region", def.groupByField)
        assertEquals(5L, def.windowMinutes)
        assertEquals("output", def.sinkTopic)
        assertEquals(1, def.steps.size)
    }

    @Test
    fun `step config defaults to empty map`() {
        val step = TopologyStep(id = "s1", type = "dedup")
        assertTrue(step.config.isEmpty())
    }

    @Test
    fun `step config preserves provided values`() {
        val step = TopologyStep(
            id = "filter",
            type = "filterByType",
            config = mapOf("allowedTypes" to listOf("page_view", "purchase"))
        )
        assertEquals("filterByType", step.type)
        assertTrue(step.config.containsKey("allowedTypes"))
    }

    @Test
    fun `data class equality works`() {
        val a = TopologyStep(id = "s1", type = "dedup")
        val b = TopologyStep(id = "s1", type = "dedup")
        assertEquals(a, b)
    }

    @Test
    fun `empty steps list is valid`() {
        val def = TopologyDefinition(
            sourceTopic = "in",
            steps = emptyList(),
            sinkTopic = "out"
        )
        assertTrue(def.steps.isEmpty())
    }
}
