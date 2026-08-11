plugins {
    kotlin("jvm") version "1.9.22"
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

    implementation("io.opentelemetry:opentelemetry-api:1.35.0")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.qameta.allure:allure-junit5:2.25.0")
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("com.platform.streams.ApplicationKt")
}
