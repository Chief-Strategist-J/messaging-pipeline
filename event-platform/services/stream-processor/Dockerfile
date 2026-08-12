FROM gradle:8.5-jdk21 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
RUN --mount=type=cache,target=/home/gradle/.gradle/caches \
    --mount=type=cache,target=/home/gradle/.gradle/wrapper \
    gradle dependencies --no-daemon || true

COPY src ./src
RUN --mount=type=cache,target=/home/gradle/.gradle/caches \
    --mount=type=cache,target=/home/gradle/.gradle/wrapper \
    gradle installDist -x test --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
ADD --chmod=644 https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.5.0/opentelemetry-javaagent.jar ./opentelemetry-javaagent.jar
COPY --from=build /app/build/install/stream-processor ./
EXPOSE 8080
ENTRYPOINT ["./bin/stream-processor"]


