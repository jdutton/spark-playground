package playground.connector

import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import scala.collection.JavaConversions._

/**
 * Create a new Kafka producer from which to put messages
 *
 * @param brokers is a list of host:port of one or more Kafka brokers (don't need to list all, just one or two)
 * @param topic that data will be posted to
 * @see
 * Blog on writing to Kafka - [[http://allegro.tech/2015/08/spark-kafka-integration.html]]
 * See also this blog - [[http://www.michael-noll.com/blog/2014/10/01/kafka-spark-streaming-integration-example-tutorial/]]
 */
class KafkaProducer(brokers: Seq[String], topic: String) {
  val props = new Properties()
  props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new org.apache.kafka.clients.producer.KafkaProducer[String, String](props)

  /**
   * Close the Kafka producer and clean up
   */
  def close(): Unit = producer.close()

  /**
   * Put each key/message string pair, where the key is used for partitioning.
   */
  def putRecords(records: Seq[ProducerRecord[String, String]]) {
    val futures = records.map { record =>
      producer.send(record, Kafka.LogErrorCallback)
    }
  }

  /**
   * Put a sequence of string messages.
   * Note the messages will be partitioned randomly since no partition key is provided.
   */
  def putValues(values: Seq[String]) {
    putRecords(values.map(v => new ProducerRecord[String, String](topic, v)))
  }

  /**
   * Put a sequence of key/message string pairs, where the key is used for partitioning.
   */
  def putKeyValues(keyValues: Seq[(String, String)]) {
    putRecords(keyValues.map(kv => new ProducerRecord[String, String](topic, kv._1, kv._2)))
  }

}
object KafkaProducer {
  def apply(brokers: Seq[String], topic: String) = new KafkaProducer(brokers, topic)
}

object Kafka {
  val defaultZKEndpoints: Seq[String] = List("127.0.0.1:2181")
  val defaultBrokers: Seq[String] = List("127.0.0.1:9092")

  def putKeyValues(brokers: Seq[String], topic: String, keyValues: Seq[(String, String)]) {
    val kafkaProducer = KafkaProducer(brokers, topic)
    kafkaProducer.putKeyValues(keyValues)
    kafkaProducer.close()
  }
  def pretty(meta: RecordMetadata): String = s"${meta.topic}:part-${meta.partition}:offset-${meta.offset}"

  object LogErrorCallback extends Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception) {
      // Note, println doesn't work from Spark Executors, so this has no effect
      Option(exception) match {
        case Some(err) => println("Send callback returns the following exception", exception)
        case None => ()
      }
    }
  }
}
