package playground

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import play.api.libs.json._
import scala.collection.immutable._

case class Tweet(text: String, hashtags: Set[String], countryCode: String, stateCode: String)
object Tweet {
  /**
   * Create a tweet from a string encoded in Twitter JSON format
   */
  def from(jsonStr: String): Option[Tweet] = {
    val tweet = Json.parse(jsonStr).asOpt[JsObject].flatMap { jsObj =>
      for {
        text <- (jsObj \ "text").asOpt[String]
        countryCode <- (jsObj \ "place" \ "country_code").asOpt[String]
        placeType <- (jsObj \ "place" \ "place_type").asOpt[String]
        fullName <- (jsObj \ "place" \ "full_name").asOpt[String]
        name <- (jsObj \ "place" \ "name").asOpt[String]
      } yield {
        val hashtags = (jsObj \ "entities" \ "hashtags" \\ "text").map(_.asOpt[String].getOrElse("")).filter(_.nonEmpty).toSet
        val stateCode = fullName.split("""\s*,\s*""").toList match {
          case city :: stateCode :: Nil if city == name && placeType == "city" => stateCode
          case _ => ""
        }
        Tweet(text = text, hashtags = hashtags, countryCode = countryCode, stateCode = stateCode)
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

  def passion(tweet: Tweet): Int = {
    val sentimentOfWord = Sentiment.sentimentOfWord
    val words = tweet.text.toLowerCase.split("""\s+""")
    val wordScores = words.map { word =>
      val wordScore = sentimentOfWord(word)
      if (wordScore < 0) -wordScore else wordScore
    }
    val passionScore: Int = wordScores.fold(0) { (total, wordScore) => total + wordScore }
    passionScore
  }

  def sentiment(tweet: Tweet): Int = {
    val sentimentOfWord = Sentiment.sentimentOfWord
    val words = tweet.text.toLowerCase.split("""\s+""")
    val sentimentScore = words.map(sentimentOfWord(_)).fold(0) { (total, wordScore) => total + wordScore }
    sentimentScore
  }

  def emotion(tweet: Tweet) = Emotion(passion = passion(tweet), sentiment = sentiment(tweet))

  // Tweets scored by the absolute value of their sentiment (i.e. passion)
  def tweetsByPassion(sc: SparkContext): RDD[(Int, Tweet)] = {
    val tweets = readTweets(sc)
    tweets.map { tweet: Tweet =>
      (passion(tweet), tweet)
    }
  }

  // Tweets scored by sentiment
  def tweetsBySentiment(sc: SparkContext): RDD[(Int, Tweet)] = {
    val tweets = readTweets(sc)
    tweets.map { tweet: Tweet =>
      (sentiment(tweet), tweet)
    }
  }

  // Tweets with emotion aggregated by state
  def tweetsByState(sc: SparkContext) = {
    val tweets = readTweets(sc)
    val stateScores = tweets
      .map { tweet => (tweet.stateCode, emotion(tweet)) }
      .reduceByKey(_ + _)
    stateScores.cache() // Small data after aggregation, so cache it to avoid processing
    stateScores
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(DefaultConf("Merica"))
    val passionateTweets = tweetsByPassion(sc).sortByKey(ascending = false).take(5)
    val positiveTweets = tweetsBySentiment(sc).sortByKey(ascending = false).take(5)
    val negativeTweets = tweetsBySentiment(sc).sortByKey(ascending = true).take(5)
    val statesByPassion = tweetsByState(sc)
    	.map { tup => (tup._2.passionScore, tup._1) }
    	.sortByKey(ascending = false)
    	.take(55)
    val statesBySentiment = tweetsByState(sc)
    	.map { tup => (tup._2.sentimentScore, tup._1) }
    	.sortByKey(ascending = false)
    	.take(55)
    println("\nPassion by state: \n" + statesByPassion.mkString("\n"))
    println("\nSentiment by state: \n" + statesBySentiment.mkString("\n"))
    println("\nMost Passionate tweets: \n" + passionateTweets.mkString("\n"))
    println("\nMost Positive tweets: \n" + positiveTweets.mkString("\n"))
    println("\nMost Negative tweets: \n" + negativeTweets.mkString("\n"))
  }
}
