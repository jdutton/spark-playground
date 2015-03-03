package playground

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import play.api.libs.json.Json
import scala.collection.immutable._
import playground.model._
import playground.connector._

object MericaStreaming {

  // This should match Merica.HDFS_INPUT_PATH
  val HDFS_OUTPUT_PATH = "spark-playground-data/merica-tweets-json/merica-tweets"

  def main(args: Array[String]) {
    val sc = new SparkContext(DefaultConf("MericaStreaming"))
    val ssc = new StreamingContext(sc, Seconds(10))

    // NOTE: these conf value must start with 'spark.*' if they are to be passed in from spark-submit --conf
    val kafkaEnabled = sc.getConf.getBoolean("spark.playground.kafka.enabled", false)
    println("Playground: Kafka is " + (if (kafkaEnabled) "ENABLED" else "DISABLED"))
    val elasticsearchEnabled = sc.getConf.getBoolean("spark.playground.es.enabled", false)
    println("Playground: Elasticsearch is " + (if (elasticsearchEnabled) "ENABLED" else "DISABLED"))
    val hdfsEnabled = sc.getConf.getBoolean("spark.playground.hdfs.enabled", false)
    println("Playground: HDFS is " + (if (hdfsEnabled) "ENABLED" else "DISABLED"))

    val trackFilters = List("merica", "america", "obama", "texas")
    val rawTweetStream = Twitter.createTweetStream(ssc, trackFilters = trackFilters)
    val rawMericaStream = rawTweetStream.filter(!_.retweet) //.filter(_.countryCode.nonEmpty)

    val rawMericaIdTweetJson: DStream[(String, String)] = rawMericaStream.map(t => t.id -> Json.toJson(t).toString)

    val mericaIdTweetStream = if (!kafkaEnabled) {
      rawMericaIdTweetJson
    } else {
      // If Kafka is enabled, let's stream our tweets to it, then stream our tweets back out from it
      val topicName = "MericaTweets"
      val topics = Map(topicName -> 1)

      rawMericaIdTweetJson.foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfTweets =>
          // It's simple, but definitely inefficient to create a new Kafka producer for each RDD partition
          Kafka.putKeyValues(Kafka.defaultBrokers, topicName, partitionOfTweets)
        }
      }

      val kafkaInput = Kafka.createInputStream(ssc, Kafka.defaultZKEndpoints, "MericaStreamingGroup", topicName)
      kafkaInput
    }

    val mericaTweetJson = mericaIdTweetStream.map(_._2)

    if (elasticsearchEnabled) {
      // If elasticsearch is enabled, let's stream our tweets to it
      import org.elasticsearch.spark.rdd.EsSpark
      import org.elasticsearch.spark._
      mericaTweetJson.foreachRDD { rdd =>
        rdd.saveJsonToEs("spark-playground/merica-streaming-tweet", Map("es.mapping.id" -> "id"))
      }
    }

    if (hdfsEnabled) {
      // Write out the full twitter JSON of the tweets.
      // This data can be processed by the Merica.main() process.
      mericaTweetJson.saveAsTextFiles(HDFS_OUTPUT_PATH)
    }

    // Just print the final input stream
    mericaIdTweetStream.print()

    ssc.start()
    ssc.awaitTermination(1000)
  }
}
