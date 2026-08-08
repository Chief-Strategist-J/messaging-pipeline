FROM gradle:8.5-jdk21 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY src ./src
RUN gradle build -x test --no-daemon

FROM eclipse-temurin:21-jre-alpine
COPY --from=build /app/build/libs/*.jar /app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app.jar"]
