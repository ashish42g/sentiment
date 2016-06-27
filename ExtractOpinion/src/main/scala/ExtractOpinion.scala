import Utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ExtractOpinion {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if(args.length < 1){
      println("Please provide the directory Path for Opinion Output in Program Arguments")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("Opinion Miner").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //Read parquet File
    val parquetFiles = sqlContext.read.parquet("file:///Users/ashish/IdeaProjects/TwitterSentimentAnalysis/OutputDir")
    val tweets = parquetFiles.map {
      row => row(1).toString
    }

    val lemNas = tweets.map(cleanData).map {
      case Some(twt) => plainTextToLemmas(twt)
      case None => ""
    }

    val mostProbOpinion = generateOpinionList(lemNas).filter(x => x._2 >= 1 && x._1.split(" ").size >= 1).take(20)

    val opinionRDD = sc.parallelize(mostProbOpinion)

    val bayesModel = NaiveBayesModel.load(sc,"/Users/ashish/IdeaProjects/TwitterSentimentAnalysis/TrainingModule/src/main/resources/Model")

    def convertOpinionData(actualData: RDD[(String, Int)]) = {
      actualData.filter(x => !x._1.isEmpty && !x._1.equals("")).map(t => (tf.transform(t._1), t._1))
    }

    val predictedOpinions = convertOpinionData(opinionRDD).map{
      x => val predictions = bayesModel.predict(x._1)
        (predictions, x._2)
    }

    val finalOpinions = predictedOpinions.groupBy(x => x._1).map(y => (y._1, y._2.map(z => z._2)))

    val sentimentDetected = finalOpinions.map(x => (x._1 match {
      case 0.0 => "Negative"
      case 1.0 => "Positive"
      case 2.0 => "Neutral"
    }, x._2.mkString(", ")))

    val formatOpinion = sentimentDetected.map(x => x._1 + "\n" + x._2)
    formatOpinion.saveAsTextFile(args(0))
    formatOpinion.foreach(println)
  }
}
