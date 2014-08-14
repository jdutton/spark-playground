package playground

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import play.api.libs.json._
import scala.collection.immutable._

case class Tweet(text: String, hashtags: Set[String])
object Tweet {
  /**
   * Create a tweet from a string encoded in Twitter JSON format 
   */
  def from(jsonStr: String): Option[Tweet] = {
    val tweet = Json.parse(jsonStr).asOpt[JsObject].flatMap { jsObj =>
      for {
        text <- (jsObj \ "text").asOpt[String]
      } yield {
        val hashtags = (jsObj \ "entities" \ "hashtags" \\ "text").map(_.asOpt[String].getOrElse("")).filter(_.nonEmpty).toSet
        Tweet(text = text, hashtags = hashtags)
      }
    }
    tweet
  }
}

object Merica {
  val twitterJsonFile = "america.txt"

  def readTweets(sc: SparkContext): RDD[Tweet] = {
    val tweetsText = sc.textFile(twitterJsonFile)
    val tweets = tweetsText.flatMap(Tweet.from(_))
    tweets.cache
  }

  // Tweets scored by the absolute value of their sentiment (i.e. passion)
  def tweetsByPassion(sc: SparkContext): RDD[(Int, Tweet)] = {
    val tweets = readTweets(sc)
    tweets.map { tweet: Tweet =>
      val sentimentOfWord = Sentiment.sentimentOfWord
      val words = tweet.text.toLowerCase.split("""\s+""")
      val wordScores = words.map { word =>
        val wordScore = sentimentOfWord(word)
        if (wordScore < 0) -wordScore else wordScore
      }
      val tweetScore: Int = wordScores.fold(0) { (total, wordScore) => total + wordScore }
      (tweetScore, tweet)
    }
  }

  // Tweets scored by sentiment
  def tweetsBySentiment(sc: SparkContext): RDD[(Int, Tweet)] = {
    val tweets = readTweets(sc)
    tweets.map { tweet: Tweet =>
      val sentimentOfWord = Sentiment.sentimentOfWord
      val words = tweet.text.toLowerCase.split("""\s+""")
      val tweetScore = words.map(sentimentOfWord(_)).fold(0) { (total, wordScore) => total + wordScore }
      (tweetScore, tweet)
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Merica")
    val sc = new SparkContext(conf)
    val passionateTweets = tweetsByPassion(sc).sortByKey(ascending = false).take(5)
    val positiveTweets = tweetsBySentiment(sc).sortByKey(ascending = false).take(5)
    val negativeTweets = tweetsBySentiment(sc).sortByKey(ascending = true).take(5)
    println("Most Passionate tweets: \n" + passionateTweets.mkString("\n"))
    println("Most Positive tweets: \n" + positiveTweets.mkString("\n"))
    println("Most Negative tweets: \n" + negativeTweets.mkString("\n"))
  }
}
