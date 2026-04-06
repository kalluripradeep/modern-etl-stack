FROM debezium/connect:2.5

USER root

# Create plugin directory and download Confluent JDBC Sink Connector + PostgreSQL JDBC Driver
RUN mkdir -p /kafka/connect/confluentinc-kafka-connect-jdbc && \
    cd /kafka/connect/confluentinc-kafka-connect-jdbc && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/10.7.4/kafka-connect-jdbc-10.7.4.jar && \
    curl -sO https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

USER kafka
