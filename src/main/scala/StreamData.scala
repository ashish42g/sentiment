import Utils._
import StatusStreamer._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization

object StreamData {

  def getLiveTweets(sc: SparkContext) = {

    val ssc = new StreamingContext(sc, Seconds(30))

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val auth = new OAuthAuthorization(twitterConfig)

    val filterFields = filterKeyWords

    val tweets = TwitterUtils.createStream(ssc, Option(auth), filterFields)

    val englishTweets = tweets.filter(_.getLang == "en").map(x => (x.getText, x.getCreatedAt, x.getGeoLocation))

    (ssc, englishTweets)
  }

  def handleStreaming(ssc: StreamingContext) = {
    ssc.start
    ssc.awaitTermination
    ssc.stop(true, true)
  }
}