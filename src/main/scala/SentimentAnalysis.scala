import java.sql.Timestamp
import java.util.Date

import Model._
import StatusStreamer._
import StreamData._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SQLContext}
import twitter4j.GeoLocation

import org.apache.spark.mllib.classification.NaiveBayesModel

object SentimentAnalysis {

  /**
    * Training Data : "0","1467810369","Mon Apr 06 22:19:45 PDT 2009","NO_QUERY","_TheSpecialOne_",
    * "@switchfoot http://twitpic.com/2y1zl - Awww, that's a bummer.  You shoulda got David Carr of Third Day to do it. ;D"
    *
    * Test Data : "4","4","Mon May 11 03:18:03 UTC 2009","kindle2","vcu451","Reading my kindle2...  Love it... Lee childs is good read."
    */

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if(args.length < 1){
      println("Please Provide Output Directory Path in Program Arguments")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("Twitter Sentiment Analyzer").setMaster("local[*]").set("spark.executor.memory", "3G").set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val trainedModel = NaiveBayesModel.load(sc, "/Users/ashish/IdeaProjects/TwitterSentimentAnalysis/TrainingModule/src/main/resources/Model")

    def displayUserTweetsOutput = {
      val userTweets = getUserTweet
      val userLabelPoint = convertUserTwitterData(sc.parallelize(userTweets))
      val userPredictedResult = predictUserLabel(userLabelPoint, trainedModel)
      println("\nSome Predictions from User Tweets:\n")
      displaySentiment(userPredictedResult)

      import sqlContext.implicits._

      userPredictedResult.map(l => UserTweet(l._1, l._2)).filter(x => !x.text.isEmpty && !x.text.equals("")).toDF.write.mode(SaveMode.Append).parquet(args(0))
      sc.stop
    }

    displayUserTweetsOutput

    def displayActualTwitterOutput = {

      val (ssc, tweetDStream) = getLiveTweets(sc)
      println("\n\n********* Predicting **********\n\n")
      val actualLabelPoint = convertActualTwitterData(tweetDStream)
      val predictedResult = predictLabel(actualLabelPoint, trainedModel)
      println("\nSome Predictions from Actual Data:\n")
      predictedResult.foreachRDD{
        rdd =>
          val predictedRDDToDisplay = rdd.map(x => (x._1, x._2, x._3))
          displaySentiment(predictedRDDToDisplay)
      }
      savePredictedAnalysis(args(0), sqlContext, predictedResult)
      handleStreaming(ssc)
    }

    //displayActualTwitterOutput

    println("\n\n********* Stopped Spark Context successfully, exiting ********")
  }

  def savePredictedAnalysis(outputPath: String, sqlContext: SQLContext, displayData: DStream[(Double, String, Vector, Date, GeoLocation)]): Unit = {
    import sqlContext.implicits._

    displayData.foreachRDD{
      rdd => rdd.map(l => Tweet(l._1, l._2.toString, new Timestamp(l._4.getTime), if(l._5 != null) l._5.toString else "")).repartition(1).filter(x => !x.text.isEmpty && !x.text.equals("")).toDF.write.mode(SaveMode.Append).parquet(outputPath)
    }
  }

  def displaySentiment(displayData: RDD[(Double, String, Vector)]) = {

    displayData.collect foreach { y =>
      println("---------------------------------------------------------------")
      println(s"Text = ${y._2}")
      println("Predicted Label = " + (if (y._1 == 1) "positive" else if (y._1 == 2) "neutral" else "negative\n"))
      println(f"Predicted Probabilities : \n Negative  : ${y._3.toArray(0)}%2.2f \n Positive  : ${y._3.toArray(1)}%2.2f \n Neutral   : ${y._3.toArray(2)}%2.2f")
      println("----------------------------------------------------------------\n\n")
    }
  }
}

case class Tweet(label: Double, text: String, dtm: Timestamp, loc: String)

case class UserTweet(label: Double, text: String)