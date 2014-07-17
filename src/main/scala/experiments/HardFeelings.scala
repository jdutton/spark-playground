package experiments

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object HardFeelings {
  val feelingWordsFile = "data/AFINN-111.txt"

  def simple(sc: SparkContext) = {
    // Input lines with TSV data with word <TAB> sentiment scores (-5 <= sentiment score <= 5)
    val sentimentLines = sc.textFile(feelingWordsFile).cache()

    // (Word, Sentiment) tuples, created from the lines of TSV data
    val sentimentByWord = sentimentLines.map { line =>
      val tokens = line.split('\t')
      (tokens(0), tokens(1).toInt)
    }

    // (Sentiment, Word) tuples (tuple is swapped in order to key by integer Sentiment score)
    val sentimentByScore = sentimentByWord.map(_.swap)

    // (Sentiment, List(Word)) tuples - group by sentiment score
    val sentimentGroups = sentimentByScore.groupByKey

    val harshWords = sentimentGroups.sortByKey(ascending = true).first._2
    val niceWords = sentimentGroups.sortByKey(ascending = false).first._2

    // Sensitive users, avert your eyes from these harsh words...
    println("Words with harshest sentiment include:\n\t" + harshWords.mkString(", "))

    // Awwwwwwwwe...
    println("Words with most positive sentiment include:\n\t" + niceWords.mkString(", "))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hard Feelings")
    val sc = new SparkContext(conf)
    simple(sc)
  }
}
