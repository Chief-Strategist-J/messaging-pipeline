package com.platform.streams

import com.platform.streams.topology.*
import com.platform.streams.serde.AvroSerdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.time.Duration
import java.util.Properties

fun main() {
    registerBuiltinSteps()

    val definition = TopologyDefinition(
        sourceTopic = Constants.TOPIC_EVENTS_RAW,
        steps = listOf(
            TopologyStep(id = Constants.STEP_DEDUP, type = Constants.STEP_DEDUP),
        ),
        groupByField = Constants.GROUP_BY_EVENT_TYPE,
        windowMinutes = Constants.DEFAULT_WINDOW_MINUTES,
        sinkTopic = Constants.TOPIC_EVENTS_ENRICHED,
    )

    val props = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.APPLICATION_ID)
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(Constants.ENV_KAFKA_BROKERS) ?: Constants.DEFAULT_KAFKA_BROKERS)
        put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Constants.STREAM_THREADS)
        put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
        put(StreamsConfig.DEFAULT_REPLICATION_FACTOR_CONFIG, Constants.REPLICATION_FACTOR)
    }

    val schemaRegistryUrl = System.getenv(Constants.ENV_SCHEMA_REGISTRY_URL) ?: Constants.DEFAULT_SCHEMA_REGISTRY_URL
    val serdes = AvroSerdes(schemaRegistryUrl)
    val streams = KafkaStreams(TopologyBuilder(definition, serdes).build(), props)
    Runtime.getRuntime().addShutdownHook(Thread { streams.close(Duration.ofSeconds(Constants.SHUTDOWN_TIMEOUT_SECONDS)) })
    streams.start()
}
