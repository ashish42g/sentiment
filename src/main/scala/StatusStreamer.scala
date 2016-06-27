import twitter4j._

import scala.collection.mutable.ListBuffer

object StatusStreamer {

  val tweetList = new ListBuffer[String]

  private val consumerKey = "dbLbzxvM3jS6eT4Wvpb64Bp9H"
  private val consumerSecret = "lMBBpI06CPxPiYJGRfMNERnYN9raRqYVpImvr4PHoOnSUqyqlu"
  private val accessToken = "413138489-3wgxDi33LRTzyjv24Xq7RbfW0Gh6XP288060H3Us"
  private val tokenSecret = "TKGREEeGoH69NBEKs1ComWzzJbLkPMbzBI9adMuYjFYIY"
  val userId = 413138489

  val twitterConfig = new twitter4j.conf.ConfigurationBuilder().setDebugEnabled(true)
    .setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(tokenSecret)
    .build

  val listener = new StatusListener() {

    def onStatus(status: Status) {
      tweetList += status.getText
    }

    override def onStallWarning(stallWarning: StallWarning): Unit = {}
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    override def onScrubGeo(l: Long, l1: Long): Unit = {}
    override def onTrackLimitationNotice(i: Int): Unit = {}
    override def onException(e: Exception) = {}
  }

  def getUserTweet = {
    val twitterStream = new TwitterStreamFactory(twitterConfig).getInstance
    twitterStream.addListener(listener)
    twitterStream.filter(new FilterQuery().follow(userId))
    Thread.sleep(90000)
    twitterStream.cleanUp
    twitterStream.shutdown
    tweetList
  }
}
