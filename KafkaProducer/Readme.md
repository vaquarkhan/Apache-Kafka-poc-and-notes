# Benchmark

To run the tests locally.

## Start kafka.

* Start two kafka broker locally.

* Create two topics `test` and `output`, with 10 partitions each.

## Start Spark.

* To run Spark locally do,

```
bin/spark-submit --class com.github.scrapcodes.kafka.SparkSQLKafkaConsumer --master local[20] --executor-memory 6G /path/Kafka-Producer-assembly.jar <broker-url>

```

## Start Kafka Producer.

This should write to `test` topic, which spark job is subscribed to. And writes to `output` topic.

```
cd Kafka-Producer

./sbt "run-main com.github.scrapcodes.kafka.LongRunningProducer <broker-url> test"

```

## Start Kafka Consumer.

This would read from the `output` topic and display statistics, by reading for 10 minutes.

```
./sbt "run-main com.github.scrapcodes.kafka.KafkaConsumerStatistics <broker-url> output /path/timeStats.txt 10

```

