plugins {
    kotlin("jvm") version "1.9.22"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    application
}

group = "com.platform"
version = "1.0.0"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:3.7.0")
    implementation("io.confluent:kafka-streams-avro-serde:7.6.0")
    implementation("org.apache.avro:avro:1.11.3")
    implementation("org.slf4j:slf4j-simple:2.0.12")
}

application {
    mainClass.set("com.platform.streams.ApplicationKt")
}
