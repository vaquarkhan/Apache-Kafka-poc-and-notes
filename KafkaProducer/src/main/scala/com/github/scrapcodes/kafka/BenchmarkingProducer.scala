package com.github.scrapcodes.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{TopicPartition, PartitionInfo}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import java.util.{Map => JMap}

/**
 * This is intended to became a high throughput producer for testing limits of rate that
 * kafka can handle and Apache Spark can consume.
 */
class BenchmarkingProducer extends AutoCloseable {
  private val (producer: KafkaProducer[String, String], consumer: KafkaConsumer[String, String]) =
    initializeClient()

  private def initializeClient(): (KafkaProducer[String, String], KafkaConsumer[String, String]) = {
    val props = new Properties()
    props.put("bootstrap.servers", "9.30.110.143:9092,9.30.110.142:9092,9.30.110.144:9092")
    // props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "163840")
    props.put("linger.ms", "1")
    props.put("group.id", "kafka1")
    props.put("compression.type", "snappy")
    props.put("buffer.memory", "3350054432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    (new KafkaProducer[String, String](props), new KafkaConsumer[String, String](props))
  }

  def detectLocalPartitions(nodeId: Int, topic: String): Set[PartitionInfo] = {
    val partitionInfoes = consumer.partitionsFor(topic).toSet
    partitionInfoes.filter(x => x.leader().id() == nodeId)
  }

  def availableRecords(topic: String): mutable.Set[(TopicPartition, Long)] = {
    consumer.subscribe(List(topic))
    //    consumer.assign(Set(new TopicPartition(topic, 0)))
    consumer.poll(0)
    val assignment: util.Set[TopicPartition] = consumer.assignment()
    if (assignment.isEmpty) println("Empty assignments")
    var count = 0l
    consumer.seekToEnd(assignment)
    val lastOffsets: mutable.Set[(TopicPartition, Long)] =
      assignment.map(p => (p, consumer.position(p)))
    println(s"Last offset: $lastOffsets")
    //    consumer.commitAsync(new OffsetCommitCallback {
    //      def onComplete(m: JMap[TopicPartition, OffsetAndMetadata], e: Exception) {
    //        println(s"Committed offsets ${m.mkString} or $e .")
    //      }
    //    })
    //    consumer.commitSync()
    lastOffsets
  }

  def readFile(path: String, numberOfLines: Int) = {

  }

  def parallelSender(count: Long, content: String, topic: String, partitions: Set[Int]) = {
    var i = 0l
    // import ExecutionContext.Implicits.global
    while (i < count && partitions.nonEmpty) {
      // Future {
      for (partition <- partitions; if i < count) {
        producer.send(new ProducerRecord[String, String](topic, partition, null, content))
        i = i + 1l
      }
      // }
    }
    producer.flush()
    i
  }

  def close() = {
    producer.close()
    consumer.close()
  }
}

object BenchmarkingProducer {


  def main(args: Array[String]): Unit = {
    val benchmarkingProducer: BenchmarkingProducer = new BenchmarkingProducer()
    var totalSentMessages = 0l
    val contentMap = scala.collection.mutable.HashMap[Int, String]()
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

    val startTime = System.currentTimeMillis()
    /** Number of messages sent per node. */
    val noOfMessages: Long = args(0).toLong
    val ignoredMessage: String = args(1)
    val topic: String = args(2)
    println("detecting...")
    val nodeIds: Set[Int] = args(3).split(",").map(_.trim.toInt).toSet
    val noOfLocalNodes = nodeIds.size
    val localPartitions: Set[(Int, Set[Int])] = nodeIds.map(id => {
      id -> benchmarkingProducer.detectLocalPartitions(id, topic).map(_.partition())
    })
    println(s"echo args $noOfMessages $ignoredMessage $topic $localPartitions ")
    var mb: Long = 0l
    var totalMb: Long = 0l
    contentMap += (0 -> content)
    contentMap += (1 -> content2)
    contentMap += (2 -> content3)
    def loop(): Unit = {
      var i = 0
      while (i < 10) {
        {
          val startTime1 = System.currentTimeMillis()
          var sentMessages = 0l
          val contentString: String = contentMap(Math.abs(Random.nextInt) % 3)
          for (((nodeId, localPartition)) <- localPartitions) {
            sentMessages += benchmarkingProducer.parallelSender(noOfMessages,
              contentString, topic, localPartition)
            totalSentMessages += sentMessages
          }
          println(s"Total no. of messages sent so far: $totalSentMessages")
          println(s"No. of messages sent in current batch: $sentMessages")
          mb = (sentMessages * contentString.length) / (1024 * 1024)
          totalMb += mb
          println(s"No. of Mega Bytes transferred in this batch: $mb MB")
          println(s"Available records:${benchmarkingProducer.availableRecords(topic)}")
          i += 1
          val endTime1 = System.currentTimeMillis()
          println(s"Rate of transfer is ${mb / ((endTime1 - startTime1) / 1000.0d)}. MB/s")
          System.gc()
        }
      }
    }
    loop()
    benchmarkingProducer.close()
    println(s"${contentMap(0).length} and ${contentMap(1).length} and ${contentMap(2).length}")
    val endTime = System.currentTimeMillis()
    println(s"Total time in seconds ${(endTime - startTime) / 1000.0d}")
    println(s"Total MB transferred:$totalMb")
    println(s"Net rate of transfer: ${totalMb / ((endTime - startTime) / 1000.0d)} MB/s ")
  }

}