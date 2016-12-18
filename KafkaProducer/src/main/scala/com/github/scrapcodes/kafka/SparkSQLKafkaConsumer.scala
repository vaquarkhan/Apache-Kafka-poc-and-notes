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

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.ProcessingTime

object SparkSQLKafkaConsumer {

  def deserialize(topic: String, data: Array[Byte]): Long = {
    if (data == null) return -999999999l // This is unexpected. but not a show stopper.
    if (data.length != 8) {
      throw new Exception("Size of data received by LongDeserializer is " + "not 8")
    }
    var value: Long = 0
    for (b <- data) {
      value <<= 8
      value |= b & 0xFF
    }
    value
  }

  val topic: String = "test"
  val outputTopic: String = "output"
  var brokerUrl: String = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      //.config("spark.sql.streaming.stateStore.maintenanceInterval", "3600s")
      //.config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "100000")
      .config("spark.sql.warehouse.dir", "/mnt/tmpfs/test")
      .config("spark.sql.streaming.checkpointLocation", "/mnt/tmpfs/")
      .appName("StructuredKafkaWordCount")
      .getOrCreate()
    brokerUrl = args(0).trim
    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("kafka")
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers", brokerUrl)
      .load()

    // Split the lines into messages
    val messages = lines.select("key", "value").as[(Array[Byte], Array[Byte])]
      .map(x => (deserialize(null, x._1), new String(x._2)))
    val meanTimeDelay:
    Dataset[(Long, String)] = messages.select(
      $"_1".as("time_sent"),
      $"_2".as("messages")).as[(Long, String)]
      // In this aggregation latency will be added ...
      //.groupBy($"messages", $"time_sent").agg($"time_sent", count($"messages").as("count"))
      //  In this aggregation, latency will be constant ...
      // .groupBy($"messages").agg(count($"time_sent"))
      .select($"time_sent", $"messages").as[(Long, String)]


    val kafkaWriter: KafkaWriter = new KafkaWriter
    val query = meanTimeDelay.writeStream.outputMode("append").trigger(ProcessingTime(0l))
      .foreach(kafkaWriter).start()

    query.awaitTermination()
    KafkaWriter.close()
  }
}

object KafkaWriter {
  private lazy val kafkaProducer = initialize(SparkSQLKafkaConsumer.brokerUrl)

  def initialize(brokerUrl: String): (KafkaProducer[Long, String]) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerUrl)
    props.put("acks", "0")
    props.put("retries", "0")
    props.put("batch.size", "100")
    props.put("linger.ms", "1")
    props.put("group.id", "kafka-spark-out-test")
    props.put("compression.type", "snappy")
    props.put("buffer.memory", "409600")
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[Long, String](props)
  }

  def close(): Unit = {
    kafkaProducer.close()
  }
}

class KafkaWriter extends ForeachWriter[(Long, String)] {
  private lazy val kafkaProducer: KafkaProducer[Long, String] = KafkaWriter.kafkaProducer

  def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  def process(value: (Long, String)): Unit = {
    val start = System.currentTimeMillis()
    kafkaProducer.send(
      new ProducerRecord[Long, String](SparkSQLKafkaConsumer.outputTopic, value._1, value._2))
    val end = System.currentTimeMillis()
    if (end - start > 5) {
      println(s"$start Send took ${end - start}ms to write to kafka for record. ${value._1}.")
    }
  }

  def close(errorOrNull: Throwable): Unit = {
    // Close is called per batch. So it is not so useful.
    val start = System.currentTimeMillis()
    kafkaProducer.flush()
    val end = System.currentTimeMillis()
    if (end - start > 5) {
      println(s"$start Flush took ${end - start}ms to flush.")
    }
  }
}