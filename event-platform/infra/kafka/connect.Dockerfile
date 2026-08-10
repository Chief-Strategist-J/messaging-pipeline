FROM confluentinc/cp-kafka-connect:7.6.0
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
ADD --chmod=644 https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.5.0/opentelemetry-javaagent.jar /usr/share/java/opentelemetry-javaagent.jar
