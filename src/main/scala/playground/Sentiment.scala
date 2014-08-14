package playground

object Sentiment {
  val feelingWordsFile = "AFINN-111.txt"
  val feelingWordsFilePath = "src/main/resources/playground/" + feelingWordsFile

  // Create an in-memory map of word -> sentiment score
  // The Map has a default sentiment score of 0 so the Map is a function (all words return a valid score),
  // rather than a partial function (only known sentiment words return a score).
  def readSentimentByWordFromResource: Map[String, Int] = {
    val in = getClass.getResourceAsStream(feelingWordsFile)
    val sentimentLines = scala.io.Source.fromInputStream(in).getLines()

    // (Word, Sentiment) tuples, created from the lines of TSV data
    val sentimentByWord: Map[String, Int] = sentimentLines.map { line =>
      val tokens = line.split('\t')
      (tokens(0), tokens(1).toInt)
    }.toMap

    sentimentByWord.withDefaultValue(0)
  }
  
  lazy val sentimentOfWord = readSentimentByWordFromResource
}