package playground

import org.apache.commons.io.FileUtils
import org.scalatest._
import playground.connector.{ Kafka, TwitterLike }
import playground.helper.KafkaUnit
import playground.model.Tweet
import scala.collection.mutable.Queue
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class TwitterMock(tweets: Seq[Tweet]) extends TwitterLike with Serializable {
  override def createTweetStream(ssc: StreamingContext, trackFilters: Seq[String]): DStream[Tweet] = {
    val tweetRDDs = tweets.map(tweet => ssc.sparkContext.parallelize(Seq(tweet), 1))
    ssc.queueStream(Queue(tweetRDDs: _*), oneAtATime = true)
  }
}

class MericaStreamingSpec extends WordSpec with Matchers with BeforeAndAfter {

  val pwd = System.getProperty("user.dir")
  val name = getClass.getSimpleName
  val (inputTopic, outputTopic) = ("MericaTweets", "MericaSentiment")

  val kafkaUnit = KafkaUnit(name = name)

  before {
    kafkaUnit.start()
    kafkaUnit.createTopic(inputTopic)
    kafkaUnit.createTopic(outputTopic)
    kafkaUnit.listTopics()
  }

  after {
    kafkaUnit.stop()
  }

  "MericaStreaming" should {

    "Stream tweets with sentiment" in {
      val waitTimeSecs = 2

      val tweets = List(
        Tweet(id = "1", hashtags = Set("#ftb"), retweet = false, countryCode = "US", stateCode = "MA", city = "Newton",
          text = "Merica, baby. Free Tom Brady. Patriots are the best.  You suck Cleveland!"),
        Tweet(id = "2", hashtags = Set("#tractorpull"), retweet = false, countryCode = "US", stateCode = "IN", city = "Springfield",
          text = "DQ tastes even better after a #tractorpull...whoot, whoot"))

      val twitterMock = new TwitterMock(tweets)
      val job = new MericaStreamingBase(twitterMock)
      job.main(
        Array(
          "--master", "local",
          "--interval-secs", 1.toString,
          "--timeout-secs", (3 * waitTimeSecs).toString,
          "--kafka-enabled",
          "--kafka-brokers", kafkaUnit.brokers.mkString(",")))

      val inputTweetCount = tweets.length
      val outputTweets = kafkaUnit.readMessagesBlocking(outputTopic, waitTimeSecs)
      val outputTweetCount = outputTweets.size // FIXME
      println(s"Read $outputTweetCount events from $outputTopic")

      outputTweetCount should ===(inputTweetCount) // all events must be passed through
    }
  }

}
