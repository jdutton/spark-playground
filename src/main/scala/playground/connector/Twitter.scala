package playground.connector

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import playground.DefaultConf
import playground.model._
import scala.collection.JavaConversions._
import scala.collection.immutable._

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

  /**
   * Create a tweet stream, applying any appropriate 'track' filters and map the results to our local Tweet model
   * @param ssc spark StreamingContext
   * @param trackFilters restrict stream to track the following list of keyword topics
   *
   * @see
   * See [[https://dev.twitter.com/streaming/overview/request-parameters#track Twitter track filtering]] for how to filter.
   * Note that location filtering is currently not supported via TwitterUtils - see [[https://github.com/apache/spark/pull/1717 Github pull request 1717]].
   */
  def createTweetStream(ssc: StreamingContext, trackFilters: Seq[String] = Nil): DStream[Tweet] = {
    readConfig()
    val twitterStream = TwitterUtils.createStream(ssc, twitterAuth = None, filters = trackFilters)
    twitterStream.flatMap(Tweet.from(_))
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(DefaultConf("Twitter"))
    val ssc = new StreamingContext(sc, Seconds(10))
    val tweetStream = createTweetStream(ssc)
    tweetStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}