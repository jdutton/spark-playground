package playground

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.immutable.Map

object HardFeelings {

  def sentimentByWord(sc: SparkContext) = {
    // Input lines with TSV data with word <TAB> sentiment scores (-5 <= sentiment score <= 5)
    val sentimentLines = sc.textFile(Sentiment.feelingWordsFilePath)

    // (Word, Sentiment) tuples, created from the lines of TSV data
    val sentimentByWord = sentimentLines.map { line =>
      val tokens = line.split('\t')
      (tokens(0), tokens(1).toInt)
    }
    sentimentByWord.cache()
    sentimentByWord
  }

  def harshestAndNicest(sc: SparkContext) = {
    val byWord = sentimentByWord(sc)

    // (Sentiment, Word) tuples (tuple is swapped in order to key by integer Sentiment score)
    val sentimentByScore = byWord.map(_.swap)

    // (Sentiment, List(Word)) tuples - group by sentiment score
    val sentimentGroups = sentimentByScore.groupByKey

    val harshWords = sentimentGroups.sortByKey(ascending = true).first._2
    val niceWords = sentimentGroups.sortByKey(ascending = false).first._2
    (harshWords, niceWords)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(DefaultConf("Hard Feelings"))
    val (harshWords, niceWords) = harshestAndNicest(sc)

    // Sensitive users, avert your eyes from these harsh words...
    println("Words with the harshest sentiment include:\n\t" + harshWords.mkString(", "))

    // Awwwwwwwwe...
    println("Words with most positive sentiment include:\n\t" + niceWords.mkString(", "))
  }
}
