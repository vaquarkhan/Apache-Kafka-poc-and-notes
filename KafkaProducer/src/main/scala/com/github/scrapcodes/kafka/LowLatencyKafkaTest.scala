/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.scrapcodes.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future, duration}
import scala.util.Try

/**
 * This test is capable to measure, the lowest possible latency with sending and receiving
 * 1K messages of about 1KB each, against any given kafka cluster. This also serves as a controlled
 * producer for conducting consumption tests against Apache Spark.
 */
class LowLatencyKafkaTest(server: String, topic: String) {

  val content =
    """|Be aware that one use case for partitions is to
      |semantically partition data, and adding partitions doesn't change the partitioning
      |of existing data so this may disturb consumers if they rely on that partition.
      |That is if data is partitioned by hash(key) % number_of_partitions then
      |this partitioning will potentially be shuffled by adding partitions but Kafka
      |will not attempt to automatically redistribute data in any way.
    """.stripMargin
  val content2 =
    """
      |Increasing the replication factor of an existing partition is easy. Just
      |specify the extra replicas in the custom reassignment json file and use it
      |with the --execute option to increase the replication factor of the specified partitions.
      |For instance, the following example increases the replication factor of partition
      |0 of topic foo from 1 to 3. Before increasing the replication factor, the partition's
      |only replica existed on broker 5. As part of increasing the replication factor, we will
      |add more replicas on brokers 6 and 7.
      | """.stripMargin
  val content3 =
    """
      |Kafka lets you apply a throttle to replication traffic, setting an upper bound on
      |the bandwidth used to move replicas from machine to machine. This is useful when
      |rebalancing a cluster, bootstrapping a new broker or adding or removing brokers, as it
      |limits the impact these data-intensive operations will have on users.
    """.stripMargin

  import ExecutionContext.Implicits.global

  def initialize(): (KafkaProducer[Long, String],
    KafkaConsumer[Long, String], KafkaConsumer[Long, String]) = {
    val props = new Properties()
    props.put("bootstrap.servers", server)
    props.put("acks", "0") // Since we are doing our own rate limiting.
    props.put("retries", "0")
    props.put("batch.size", "100")
    props.put("linger.ms", "1")
    props.put("group.id", "kafka-low-latency-test")
    props.put("compression.type", "snappy")
    props.put("buffer.memory", "40960")
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer")
    (new KafkaProducer[Long, String](props),
      new KafkaConsumer[Long, String](props),
      new KafkaConsumer[Long, String](props))
  }

  private val (producer: KafkaProducer[Long, String],
  consumer1: KafkaConsumer[Long, String],
  consumer2: KafkaConsumer[Long, String]) =
    initialize()
  private var producerStartTime = 0l
  private var consumerStartTime = 0l

  import duration._

  def producer(topic: String, timeToRun: Duration = 2.days): Future[Unit] = {
    val oneKBMessage = content + content2 + content3 // This is little over 1KB.
    def payload(id: Int, ts: Long) = s"{id:$id, timestamp:$ts, content: $oneKBMessage }"
    val durationNanos: Long = timeToRun.toNanos
    val endTime = System.nanoTime() + durationNanos
    Future {
      val startTime: Long = System.nanoTime()
      var x = startTime
      producerStartTime = startTime
      println(s"producer start time : $startTime")
      while (endTime > x) {
        // Send 100 messages in one go.
        x = System.currentTimeMillis()
        for (i <- 0 to 10) {
          val nanoTime: Long = System.nanoTime()
          producer.send(new ProducerRecord[Long, String](topic, nanoTime, payload(i, nanoTime)))
        }
        // flush, should not have any effect, if we have batch.size as 1.
        producer.flush()
        val y = System.currentTimeMillis()
        val ellapsedTime = y - x
        if (ellapsedTime >= 10) {
          println(s"WARN: taking too long to send messages($ellapsedTime)," +
            s" tune batch size and acks.")
        } else {
          Thread.sleep(10 - ellapsedTime) // To rate limit to about 1K msg/sec.
        }

      }
    }
  }

  def consumer(topic: String, duration: Duration): Future[List[Long]] = {
    consumer1.subscribe(List(topic))
    val list: ListBuffer[Long] = new mutable.ListBuffer[Long]()
    val _list: ListBuffer[Long] = new mutable.ListBuffer[Long]()
    val durationNanos: Long = duration.toNanos
    val nanos1Min = 1.minute.toNanos
    val endTime = System.nanoTime() + durationNanos
    var x = System.nanoTime()
    val f: Future[List[Long]] = Future {
      var time = System.nanoTime()
      println(s"consumer1 start time : $time")
      consumerStartTime = time
      while (endTime > time) {
        val records: ConsumerRecords[Long, String] = consumer1.poll(0)
        time = System.nanoTime()
        for (rec: ConsumerRecord[Long, String] <- records.toIterable) {
          // toIterable is O(1)
          val totalTimeDelay: Long = time - rec.key()
          _list += totalTimeDelay
        }
        consumer1.commitAsync()

        if (x < time) {
          val start = System.currentTimeMillis()
          list ++= _list
          println("For current batch:")
          Future(KafkaConsumerStatistics.showStats(_list.clone().toList))
          println("For all batches received so far:")
          Future(KafkaConsumerStatistics.showStats(list.clone().toList))
          val end = System.currentTimeMillis()
          println(s"Time lost in showing stats: ${end - start}ms")
          x = time + nanos1Min
          _list.clear()
        }
      }
      list ++= _list
      list.toList
    }
    f
  }

  /** Forward every message on input to output */
  def consumerProducer(topic: String, outTopic: String, duration: Duration): Future[Unit] = {
    consumer2.subscribe(List(topic))
    val list: ListBuffer[Long] = new mutable.ListBuffer[Long]()
    val durationNanos: Long = duration.toNanos
    val endTime = System.nanoTime() + durationNanos
    Future {
      var time = System.nanoTime()
      println(s"consumerP start time : $time")
      consumerStartTime = time
      while (endTime > time) {
        val records: ConsumerRecords[Long, String] = consumer2.poll(0)
        time = System.nanoTime()
        for (rec: ConsumerRecord[Long, String] <- records.toIterable) {
          producer.send(new ProducerRecord[Long, String](outTopic, rec.key(), rec.value()))
        }
        producer.flush()
        consumer2.commitAsync()
      }
    }
  }

  def close(): Unit = {
    val delayStartThreads: Long = (producerStartTime - consumerStartTime).nanos.toMillis
    println(s"Delay in starting consumer1 is: $delayStartThreads millis.")
    producer.close()
    consumer1.close()
    consumer2.close()
  }


}

/** Can be run to benchmark kafka producer and consumer. */
object LowLatencyKafkaBenchmarkTest {
  def main(args: Array[String]) = {
    import duration._

    val brokerUrl: String = args(0).trim
    val statsFile: String = args(1).trim
    val inputTopic: String = args(2).trim
    val outputTopic: String = args(3).trim
    val timeToPerformTheRun = args(4).trim.toInt.minutes

    val lowLatencyKafkaTest: LowLatencyKafkaTest = new LowLatencyKafkaTest(brokerUrl,
      inputTopic)
    val consumerThread: Future[List[Long]] = lowLatencyKafkaTest.consumer(outputTopic,
      timeToPerformTheRun)
    // For running the latency test against Kafka.
    val consumerProducerThread = lowLatencyKafkaTest.consumerProducer(inputTopic, outputTopic,
      timeToPerformTheRun)
    val producerThread: Future[Unit] = lowLatencyKafkaTest.producer(inputTopic, timeToPerformTheRun)

    Try(Await.ready(consumerProducerThread, timeToPerformTheRun))
    Try(Await.ready(producerThread, timeToPerformTheRun)) // Since this is interrupted.

    val timeDelayList = Await.result(consumerThread, timeToPerformTheRun + 2000
      .milliseconds)
    KafkaConsumerStatistics.showStats(timeDelayList, Some(25))
    lowLatencyKafkaTest.close()
    import java.io.File

    val file: File = new File(statsFile)
    KafkaConsumerStatistics.printToFile(file)(p => timeDelayList.foreach(x => p.println(x.nanos
      .toMillis)))

  }
}

object LongRunningProducer {
  def main(args: Array[String]) {
    import duration._
    val brokerUrl: String = args(0).trim
    val outputTopic: String = args(1).trim
    val timeToPerformTheRun = Try(args(2).trim.toInt.minutes).toOption.getOrElse(2.days)
    val lowLatencyKafkaTest: LowLatencyKafkaTest = new LowLatencyKafkaTest(brokerUrl,
      outputTopic)
    val producerThread: Future[Unit] =
      lowLatencyKafkaTest.producer(outputTopic, timeToPerformTheRun)
    // We ignore the exception, since this thread is interrupted.
    Try(Await.ready(producerThread, timeToPerformTheRun))
  }
}

/* For testing consumer in a different JVM or machine. */
object KafkaConsumerStatistics {
  def main(args: Array[String]) = {
    import duration._
    val brokerUrl: String = args(0).trim
    val topic: String = args(1).trim
    val statsFile: String = args(2).trim
    val timeToPerformTheRun = args(3).trim.toInt.minutes
    println(s"Running for ${timeToPerformTheRun.toMinutes}minutes for topic $topic.")
    val lowLatencyKafkaTest: LowLatencyKafkaTest = new LowLatencyKafkaTest(brokerUrl,
      topic)
    val ignoredConsumer = lowLatencyKafkaTest.consumer(topic, 119
      .seconds)
    Await.ready(ignoredConsumer, 2.minutes) // ignore first 1 minute content as it distorts the
    println("Ignored all records uptil now.")
    // result.
    val consumerThread: Future[List[Long]] = lowLatencyKafkaTest.consumer(topic,
        timeToPerformTheRun)
    val listLong: ListBuffer[Long] = mutable.ListBuffer[Long]()

    Await.result(consumerThread, timeToPerformTheRun + 2000.milliseconds)
      .foreach(x => listLong.append(x))

    showStats(listLong.toList, Some(25))
    import java.io.File
    val file: File = new File(statsFile)
    printToFile(file)(p => listLong.foreach(x => p.println(x.nanos.toMillis)))
    lowLatencyKafkaTest.close()
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  def showStats(timeDelayList: List[Long], coefficientOption: Option[Int] = None): Unit = {
    import duration._
    val oneMs = 1.milliseconds.toNanos
    def filter(i: Int): Int = timeDelayList.count(x => x < (oneMs * i * 2) && x > (oneMs * i))
    val average: Int = (timeDelayList.sum / timeDelayList.size).nanos.toMillis.toInt
    val coefficient: Int = coefficientOption.getOrElse(average / 2)
    val highCount: Int = timeDelayList.count(_ > (oneMs * coefficient * 32))
    println(
      s"""Statistics:
          |Total number of records: ${timeDelayList.count(_ => true)}
          |Max:${timeDelayList.max.nanos.toMillis}ms
          |Min:${timeDelayList.min.nanos.toMillis}ms
          |Average:${average}ms
          |Number of records in > ${coefficient * 32}ms: $highCount
          |Number of records in < ${coefficient * 32}ms: ${filter(coefficient * 16)}
          |Number of records in < ${coefficient * 16}ms: ${filter(coefficient * 8)}
          |Number of records in < ${coefficient * 8}ms: ${filter(coefficient * 4)}
          |Number of records in < ${coefficient * 4}ms: ${filter(coefficient * 2)}
          |Number of records in < ${coefficient * 2}ms: ${filter(coefficient)}
          |Number of records in < ${coefficient}ms: ${filter(coefficient / 2)}
          |Number of records in < ${coefficient / 2}ms: ${filter(coefficient / 4)}
          |Number of records in < 10ms: ${timeDelayList.count(_ < (oneMs * 10))}
          |""".stripMargin)
  }
}