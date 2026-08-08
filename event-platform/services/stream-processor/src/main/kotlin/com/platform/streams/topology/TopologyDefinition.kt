package com.platform.streams.topology

data class TopologyStep(
    val id: String,
    val type: String,
    val config: Map<String, Any> = emptyMap()
)

data class TopologyDefinition(
    val sourceTopic: String,
    val steps: List<TopologyStep>,
    val groupByField: String = "eventType",
    val windowMinutes: Long = 1,
    val sinkTopic: String
)
