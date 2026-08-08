package com.platform.streams.topology

import com.platform.streams.serde.AvroSerdes
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertNotNull

class TopologyBuilderIntegrationTest {

    @BeforeEach
    fun setup() {
        registerBuiltinSteps()
    }

    @Test
    fun `topology builds successfully with valid definition`() {
        val definition = TopologyDefinition(
            sourceTopic = "events.raw",
            steps = listOf(
                TopologyStep(id = "dedup", type = "dedup"),
            ),
            groupByField = "eventType",
            windowMinutes = 1,
            sinkTopic = "events.enriched"
        )
        val serdes = AvroSerdes("http://localhost:8081")
        val builder = TopologyBuilder(definition, serdes)
        val topology = builder.build()
        assertNotNull(topology)
        assertNotNull(topology.describe())
    }

    @Test
    fun `topology builds with empty steps`() {
        val definition = TopologyDefinition(
            sourceTopic = "events.raw",
            steps = emptyList(),
            sinkTopic = "events.enriched"
        )
        val serdes = AvroSerdes("http://localhost:8081")
        val builder = TopologyBuilder(definition, serdes)
        val topology = builder.build()
        assertNotNull(topology)
    }

    @Test
    fun `topology builds with multiple steps`() {
        val definition = TopologyDefinition(
            sourceTopic = "events.raw",
            steps = listOf(
                TopologyStep(id = "dedup", type = "dedup"),
                TopologyStep(
                    id = "filter",
                    type = "filterByType",
                    config = mapOf("allowedTypes" to listOf("page_view"))
                ),
            ),
            groupByField = "eventType",
            windowMinutes = 5,
            sinkTopic = "events.enriched"
        )
        val serdes = AvroSerdes("http://localhost:8081")
        val builder = TopologyBuilder(definition, serdes)
        val topology = builder.build()
        assertNotNull(topology)
    }
}
