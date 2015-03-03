package playground.connector

import collection.immutable._
import collection.JavaConverters._
import java.util.Properties
import kafka.producer._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.kafka.KafkaInputDStream

/**
 * Create a new Kafka producer from which to put messages
 *
 * @param brokers is a list of host:port of one or more Kafka brokers (don't need to list all, just one or two)
 * @param topic that data will be posted to
 * @see
 * Spark has an example [[https://github.com/apache/spark/blob/master/examples/scala-2.10/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala KafkaWordCount]]
 * that was referenced to create this class.
 * See also this blog - [[http://www.michael-noll.com/blog/2014/10/01/kafka-spark-streaming-integration-example-tutorial/ Integrating Kafka and Spark Streaming]]
 */
class KafkaProducer(brokers: Seq[String], topic: String) {
  val props = new Properties()
  props.put("metadata.broker.list", brokers.mkString(","))
  props.put("serializer.class", "kafka.serializer.StringEncoder")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)

  /**
   * Put a sequence of string messages.
   * Note the messages will be partitioned randomly since no partition key is provided.
   */
  def putValues(values: Seq[String]) {
    putValues(values.toIterator)
  }

  /**
   * Put each string message from an iterator.
   * Note the messages will be partitioned randomly since no partition key is provided.
   */
  def putValues(valueIter: Iterator[String]) {
    for (v <- valueIter) {
      val keyedMsg = new KeyedMessage[String, String](topic, v)
      producer.send(keyedMsg)
    }
  }

  /**
   * Put a sequence of key/message string pairs, where the key is used for partitioning.
   */
  def putKeyValues(keyValues: Seq[(String, String)]) {
    putKeyValues(keyValues.toIterator)
  }

  /**
   * Put each key/message string pair from an iterator, where the key is used for partitioning.
   */
  def putKeyValues(keyValueIter: Iterator[(String, String)]) {
    for (kv <- keyValueIter) {
      val keyedMsg = new KeyedMessage[String, String](topic, kv._1, kv._2)
      producer.send(keyedMsg)
    }
  }

  /**
   * Close the Kafka producer and clean up
   */
  def close(): Unit = producer.close()

}
object KafkaProducer {
  def apply(brokers: Seq[String], topic: String) = new KafkaProducer(brokers, topic)
}

object Kafka {

  val defaultZKEndpoints: Seq[String] = List("localhost:2181")
  val defaultBrokers: Seq[String] = List("localhost:9092")

  def createInputStream(ssc: StreamingContext, zk: Seq[String], group: String, topics: Map[String, Int]) = {
    val topicsJavaMap = topics.asJava
    KafkaUtils.createStream(ssc, zk.mkString(","), group, topics, StorageLevel.MEMORY_AND_DISK)
  }
  def createInputStream(ssc: StreamingContext, zk: Seq[String], group: String, topic: String) = {
    val topics = Map(topic -> 1)
    KafkaUtils.createStream(ssc, zk.mkString(","), group, topics, StorageLevel.MEMORY_AND_DISK)
  }

  def putKeyValues(brokers: Seq[String], topic: String, keyValueIter: Iterator[(String, String)]) {
    val kafkaProducer = KafkaProducer(brokers, topic)
    kafkaProducer.putKeyValues(keyValueIter)
    kafkaProducer.close()
  }

  def putKeyValues(brokers: Seq[String], topic: String, keyValues: Seq[(String, String)]) {
    putKeyValues(brokers, topic, keyValues.toIterator)
  }
}