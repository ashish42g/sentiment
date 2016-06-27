import Testing._
import Training._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TrainModel {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Train Naive Bayes Model").setMaster("local[*]").set("spark.executor.memory", "3G").set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)

    val trainingLabelPoint = createTrainingLabel(sc)
    val testingData = createTestingLabel(sc)

    val trainedModel = modelTraining(trainingLabelPoint)
    val initialTestingResult = modelTesting(testingData, trainedModel)

    val initialAccuracy = 100.0 * initialTestingResult.filter(x => x._1 == x._2).count() / testingData.count
    println(f"Training and Testing complete. Initial Accuracy is = $initialAccuracy%2.2f\n")

    Thread.sleep(5000)

    val finalModel = if (initialAccuracy < 80.0) {

      println("Accuracy is less than 80% .....\n")
      println("\n\n********* Re - Training to increase Efficiency **********\n\n")

      val testingLabelPoint = testingData.map(_._1)
      val reTrainedModel = modelTraining(trainingLabelPoint ++ testingLabelPoint)
      val finalTestResult = modelTesting(testingData, reTrainedModel)

      val finalAccuracy = 100.0 * finalTestResult.filter(x => x._1 == x._2).count() / testingData.count
      println(f"Training and Testing complete. Final Accuracy is = $finalAccuracy%2.2f\n")
      reTrainedModel
    } else trainedModel

    finalModel.save(sc, "/Users/ashish/IdeaProjects/TwitterSentimentAnalysis/TrainingModule/src/main/resources/Model")
    println("\n *********** The Model is Trained and Saved ************\nPlease run the Live Sentiment Analysis Module Now")
  }
}
