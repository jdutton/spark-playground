package playground.model

import play.api.libs.json._

case class Tweet(text: String, hashtags: Set[String], countryCode: String, stateCode: String) {

  lazy val passion: Int = Sentiment.passion(text)

  lazy val sentiment: Int = Sentiment.sentiment(text)

  lazy val emotion = Emotion(passion = passion, sentiment = sentiment)
}

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
