FROM confluentinc/cp-kafka-connect:latest
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
