package playground

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.immutable._
import playground.model._

object Merica {
  val twitterJsonFile = "america.txt"

  def readTweets(sc: SparkContext): RDD[Tweet] = {
    val tweetsText = sc.textFile(twitterJsonFile)
    val tweets = tweetsText.flatMap(Tweet.from(_))
    tweets.cache
  }

  // Tweets scored by the absolute value of their sentiment (i.e. passion)
  def tweetsByPassion(tweets: RDD[Tweet]): RDD[(Int, Tweet)] = {
    tweets.map { tweet: Tweet =>
      (tweet.passion, tweet)
    }
  }

  // Tweets scored by sentiment
  def tweetsBySentiment(tweets: RDD[Tweet]): RDD[(Int, Tweet)] = {
    tweets.map { tweet: Tweet =>
      (tweet.sentiment, tweet)
    }
  }

  // Tweets with emotion aggregated by state
  def tweetsByState(tweets: RDD[Tweet]) = {
    val stateScores = tweets
      .map { tweet => (tweet.stateCode, tweet.emotion) }
      .reduceByKey(_ + _)
    stateScores.cache() // Small data after aggregation, so cache it to avoid processing
    stateScores
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(DefaultConf("Merica"))
    val tweets = readTweets(sc)
    val passionateTweets = tweetsByPassion(tweets).sortByKey(ascending = false).take(5)
    val positiveTweets = tweetsBySentiment(tweets).sortByKey(ascending = false).take(5)
    val negativeTweets = tweetsBySentiment(tweets).sortByKey(ascending = true).take(5)
    val statesByPassion = tweetsByState(tweets)
      .map { tup => (tup._2.passionScore, tup._1) }
      .sortByKey(ascending = false)
      .take(55)
    val statesBySentiment = tweetsByState(tweets)
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
