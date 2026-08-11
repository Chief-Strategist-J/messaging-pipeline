package com.platform.streams

object Constants {
    const val DEFAULT_KAFKA_BROKERS = "kafka:9092"
    const val DEFAULT_SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

    const val APPLICATION_ID = "stream-processor"
    const val STREAM_THREADS = 4
    const val REPLICATION_FACTOR = 1
    const val SHUTDOWN_TIMEOUT_SECONDS = 10L

    const val ENV_KAFKA_BROKERS = "KAFKA_BROKERS"
    const val ENV_SCHEMA_REGISTRY_URL = "SCHEMA_REGISTRY_URL"

    const val TOPIC_EVENTS_RAW = "events.raw"
    const val TOPIC_EVENTS_ENRICHED = "events.enriched"

    const val GROUP_BY_EVENT_TYPE = "eventType"
    const val DEFAULT_WINDOW_MINUTES = 1L

    const val STEP_DEDUP = "dedup"
    const val STEP_FILTER_BY_TYPE = "filterByType"

    const val CONFIG_KEY_ALLOWED_TYPES = "allowedTypes"
    const val CONFIG_KEY_SCHEMA_REGISTRY_URL = "schema.registry.url"
}
