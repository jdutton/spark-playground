package playground.connector

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import playground.DefaultConf
import scala.collection.JavaConversions._

object Twitter {

  /**
   * Read twitter config properties from `src/main/resources/twitter.conf`, for example:
   *   twitter4j.oauth.consumerKey="abcd"
   *   twitter4j.oauth.consumerSecret="ABCDEFGHIJ"
   *   twitter4j.oauth.accessToken="106237895"
   *   twitter4j.oauth.accessTokenSecret="8hADNUnhsk"
   */
  def readConfig() {
    val props = List(
      "twitter4j.oauth.consumerKey",
      "twitter4j.oauth.consumerSecret",
      "twitter4j.oauth.accessToken",
      "twitter4j.oauth.accessTokenSecret")
    val twitterConfig = ConfigFactory.load("twitter")
    val referenceConfigMap = props.map(_ -> "").toMap
    val referenceConfig = ConfigFactory.parseMap(referenceConfigMap)
    twitterConfig.checkValid(referenceConfig)
    props.foreach { prop =>
      System.setProperty(prop, twitterConfig.getString(prop))
    }
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(DefaultConf("Twitter"))
    val ssc = new StreamingContext(sc, Seconds(10))
    readConfig()
    val twitterStream = TwitterUtils.createStream(ssc, None)
    twitterStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}