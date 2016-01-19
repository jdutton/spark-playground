package playground

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka._
import play.api.libs.json.Json
import playground._
import playground.model._
import playground.connector._

object MericaStreaming extends MericaStreamingBase(twitter = Twitter)

class MericaStreamingBase(twitter: TwitterLike) extends Serializable {
  case class Opts(
      master: String = "", // If non-empty, job will set the master, otherwise expect spark-submit to set it
      intervalSecs: Int = 10,
      timeoutSecs: Int = 120,
      kafkaEnabled: Boolean = false,
      elasticsearchEnabled: Boolean = false,
      hdfsEnabled: Boolean = false,
      brokers: Seq[String] = Kafka.defaultBrokers) {

    def parse(args: Vector[String]): Opts = args match {
      case Vector() =>
        this // Base case - nothing left to parse, just return this
      case "--master" +: master +: otherArgs =>
        this.copy(master = master).parse(otherArgs)
      case "--interval-secs" +: interval +: otherArgs =>
        this.copy(intervalSecs = interval.toInt).parse(otherArgs)
      case "--timeout-secs" +: timeout +: otherArgs =>
        this.copy(timeoutSecs = timeout.toInt).parse(otherArgs)
      case "--kafka-brokers" +: b +: otherArgs =>
        val brokers = b.split(",").toSeq.map(_.trim).filter(_.nonEmpty)
        this.copy(brokers = brokers).parse(otherArgs)
      case "--kafka-enabled" +: otherArgs =>
        this.copy(kafkaEnabled = true).parse(otherArgs)
      case "--es-enabled" +: otherArgs =>
        this.copy(elasticsearchEnabled = true).parse(otherArgs)
      case "--hdfs-enabled" +: otherArgs =>
        this.copy(hdfsEnabled = true).parse(otherArgs)
      case unknownOpt +: otherArgs if unknownOpt.startsWith("-") =>
        println(s"Unknown option $unknownOpt!")
        this.parse(otherArgs)
    }
  }

  val (inputTopic, outputTopic) = ("MericaTweets", "MericaSentiment")

  // This should match Merica.HDFS_INPUT_PATH
  val HDFS_OUTPUT_PATH = "spark-playground-data/merica-tweets-json/merica-tweets"

  def main(args: Array[String]) {
    val opts = Opts().parse(args.toVector)
    val sc = new SparkContext(DefaultConf("MericaStreaming", master = opts.master))
    val ssc = new StreamingContext(sc, Seconds(opts.intervalSecs))

    // NOTE: these conf value must start with 'spark.*' if they are to be passed in from spark-submit --conf
    val kafkaEnabled = sc.getConf.getBoolean("spark.playground.kafka.enabled", opts.kafkaEnabled)
    println("Playground: Kafka is " + (if (kafkaEnabled) s"ENABLED (brokers=${opts.brokers.mkString(",")})" else "DISABLED"))
    val elasticsearchEnabled = sc.getConf.getBoolean("spark.playground.es.enabled", opts.elasticsearchEnabled)
    println("Playground: Elasticsearch is " + (if (elasticsearchEnabled) "ENABLED" else "DISABLED"))
    val hdfsEnabled = sc.getConf.getBoolean("spark.playground.hdfs.enabled", opts.hdfsEnabled)
    println("Playground: HDFS is " + (if (hdfsEnabled) "ENABLED" else "DISABLED"))
    val easyWay = sc.getConf.getBoolean("spark.playground.easy", true)
    println("Playground: Sentiment calculation will be performed the " + (if (easyWay) "EASY" else "HARD") + " way")

    val trackFilters = List("merica", "america", "obama", "texas")
    val rawTweetStream: DStream[Tweet] = twitter.createTweetStream(ssc, trackFilters = trackFilters)
    val rawMericaStream = rawTweetStream.filter(!_.retweet) //.filter(_.countryCode.nonEmpty)

    val rawMericaIdTweetJson: DStream[(String, String)] = rawMericaStream.map(t => t.id -> Json.toJson(t).toString)

    val mericaIdTweetStream = if (!kafkaEnabled) {
      rawMericaIdTweetJson
    } else {
      // If Kafka is enabled, let's stream our tweets to it, then stream our tweets back out from it
      val topics = Map(inputTopic -> 1)

      rawMericaIdTweetJson.foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfTweets =>
          val tweets = partitionOfTweets.toArray
          // It's simple, but definitely inefficient to create a new Kafka producer for each RDD partition
          Kafka.putKeyValues(opts.brokers, inputTopic, tweets)
        }
      }

      // Create direct kafka stream with brokers and topics
      val kafkaParams = Map[String, String]("metadata.broker.list" -> opts.brokers.mkString(","))
      val kafkaInput = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(inputTopic))
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

    // Sentiment analyze the tweets

    val bufferedTweetStream: DStream[Tweet] = mericaIdTweetStream.flatMap {
      case (id, json) => Json.parse(json).asOpt[Tweet]
    }

    // ( word -> sentimentScore ) used as a batch RDD input to the streaming RDD transformation below
    val sentimentByWord = HardFeelings.sentimentByWord(sc)

    // ( sentimentScore -> Tweet )
    val mericaTweetSentimentStream = bufferedTweetStream.transform { tweets => Merica.tweetsBySentiment(tweets, sentimentByWord, easyWay) }

    // Just print the final input stream
    mericaTweetSentimentStream.foreachRDD { rdd =>
      rdd.foreachPartition { tweetsPartition =>
        val tweets = tweetsPartition.toArray.map {
          case (sentiment, tweet) =>
            val tweetJsonStr = (tweet.toJson ++ Json.obj("sentiment" -> sentiment.toString)).toString
            tweet.id -> tweetJsonStr
        }
        if (kafkaEnabled && tweets.nonEmpty) {
          Kafka.putKeyValues(opts.brokers, outputTopic, tweets)
        }
        tweets.foreach(println)
      }
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(opts.timeoutSecs * 1000L)
  }
}
