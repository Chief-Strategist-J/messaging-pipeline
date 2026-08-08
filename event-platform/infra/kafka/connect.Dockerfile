FROM quay.io/debezium/connect:latest
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
