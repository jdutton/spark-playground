package playground

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._
import play.api.libs.json.Json
import scala.collection.immutable._
import playground.model._
import playground.connector._

object Merica {

  // This should match MericaStreaming.HDFS_OUTPUT_PATH
  val HDFS_INPUT_PATH = "spark-playground-data/merica-tweets-json/*"

  def readTweets(sc: SparkContext): RDD[Tweet] = {
    val tweetsText = sc.textFile(HDFS_INPUT_PATH)
    val tweets = tweetsText.flatMap(Json.parse(_).asOpt[Tweet])
    tweets.cache
  }

  // Tweets scored by the absolute value of their sentiment (i.e. passion)
  def tweetsByPassion(tweets: RDD[Tweet]): RDD[(Int, Tweet)] = {
    tweets.map { tweet: Tweet =>
      (tweet.passion, tweet)
    }
  }

  /**
   * Calculate Tweet sentiment using an RDD of tweets and an RDD of (word -> sentimentScore).
   *
   * This is crazy inefficient compared to loading the words/sentiment into memory and calculating,
   * but this highlights using a distributed algorithm to solve the calculation.
   *
   * Note: setting easyWay=true just solves for sentiment in memory (rather than Spark).
   *
   */
  def tweetsBySentiment(tweets: RDD[Tweet], sentimentByWord: RDD[(String, Int)], easyWay: Boolean = false): RDD[(Int, Tweet)] = {
    if (easyWay) {
      // Compute the sentiment directly in memory (rather than distributing the calculation to Spark)
      tweets.map { tweet: Tweet =>
        (tweet.sentiment, tweet)
      }
    } else {
      // (tweetId -> Tweet)
      val tweetsById = tweets.map { tweet => tweet.id -> tweet }

      // ( word -> tweetId )
      val tweetWordsById = tweets.flatMap { tweet => tweet.words.map(_ -> tweet.id) }

      // ( word -> (tweetId -> sentimentScore) )
      val joinedWords = tweetWordsById join sentimentByWord

      // ( tweetId -> sentimentScore )
      val tweetIdToSentiment = joinedWords.map(_._2)

      // ( tweetId -> sentimentScore )
      val tweetIdToTotalSentiment = tweetIdToSentiment.groupByKey.map(tup => tup._1 -> tup._2.sum)

      // ( tweetId -> ( sentimentScore -> Tweet ) )
      val joinedSentimentTweets = tweetIdToTotalSentiment join tweetsById

      // ( sentimentScore -> Tweet )
      val sentimentTweets = joinedSentimentTweets.map(_._2)

      sentimentTweets
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

  def tweetsToES(tweets: RDD[Tweet]) {
    // FIXME: Below fails at run-time due to "task not serializable" exception
    //Elasticsearch.write(tweets, "spark-playground/tweet", Map("es.mapping.id" -> "id"))
    val jsonStrings = tweets.map(Json.toJson(_).toString)
    jsonStrings.saveJsonToEs("spark-playground/tweet", Map("es.mapping.id" -> "id"))
  }

  def tweetsFromES(sc: SparkContext, query: String = ""): RDD[Tweet] = {
    // FIXME: Below fails at run-time due to "task not serializable" exception
    //Elasticsearch.read(sc, "spark-playground/tweet")

    // ES-Hadoop BUG: the last returned JSON string has trailing "}]}", for example `{"id":1,"text":"Hello World"}}]}`
    sc.esJsonRDD(resource = "spark-playground/tweet", query = query).values.flatMap(Json.parse(_).asOpt[Tweet])
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(DefaultConf("Merica"))

    // NOTE: these conf value must start with 'spark.*' if they are to be passed in from spark-submit --conf
    val elasticsearchEnabled = sc.getConf.getBoolean("spark.playground.es.enabled", false)
    println("Playground: Elasticsearch is " + (if (elasticsearchEnabled) "ENABLED" else "DISABLED"))
    val easyWay = sc.getConf.getBoolean("spark.playground.easy", true)
    println("Playground: Sentiment calculation will be performed the " + (if (easyWay) "EASY" else "HARD") + " way")

    val tweetsTextFile = readTweets(sc)

    if (elasticsearchEnabled) {
      tweetsToES(tweetsTextFile)
      Thread.sleep(2) // ES doesn't index immediately
    }

    val tweets = if (elasticsearchEnabled) {
      // BUG: ES-Hadoop bug (see above) prevents reading from ES as JSON at the moment
      //tweetsFromES(sc, "?q=text:huh")
      tweetsTextFile
    } else {
      tweetsTextFile
    }

    val passionateTweets = tweetsByPassion(tweets).sortByKey(ascending = false).take(5)
    val statesByPassion = tweetsByState(tweets)
      .map { tup => (tup._2.passionScore, tup._1) }
      .sortByKey(ascending = false)
      .take(55)

    // ( word -> sentimentScore )
    val sentimentByWord = HardFeelings.sentimentByWord(sc)

    // ( sentimentScore -> Tweet )
    val sentimentTweets = tweetsBySentiment(tweets, sentimentByWord, easyWay = easyWay)

    //Delete file first, to prevent failures...
    val sentimentTweetOutputDir = "tweets-by-sentiment"
    //sentimentTweets.saveAsTextFile(sentimentTweetOutputDir)
    val positiveTweets = sentimentTweets.sortByKey(ascending = false).take(5)
    val negativeTweets = sentimentTweets.sortByKey(ascending = true).take(5)
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
