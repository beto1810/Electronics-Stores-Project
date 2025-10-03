FROM confluentinc/cp-kafka-connect:7.6.1
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/