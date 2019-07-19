## Create topic

```bash
$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic Passenger
$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic PassengerCount
```

## Start console producer

```bash
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Passenger
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic PassengerCount
```

## Start console consumer

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Passenger
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic PassengerCount
```
