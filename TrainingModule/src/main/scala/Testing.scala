import Model._
import Utils._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Testing {

  def createTestingLabel(sc: SparkContext) = {
    val testData = sc.textFile(getClass.getClassLoader.getResource("TestData.csv").getPath)
    val parsedData = parseFile(testData)

    val testingVector = parsedData.map(t => (t._1, tf.transform(cleanseData(t._2)), t._2))

    testingVector.map(x => {
      val lp = new LabeledPoint(x._1.toDouble, x._2)
      (lp, x._3)
    })
  }

  def modelTesting(test_label: RDD[(LabeledPoint, String)], model: NaiveBayesModel) = {

    println("\n\n********* Testing **********\n\n")
    time {
      test_label.map(p => {
        val labeledPoint = p._1
        val text = p._2
        val features = labeledPoint.features
        val actual_label = labeledPoint.label
        val predicted_label = model.predict(features)
        val predicted_prob = model.predictProbabilities(features)
        (actual_label, predicted_label, text, predicted_prob)
      })
    }
  }

  def displayTestSentiment(displayData: RDD[(Double, Double, String, Vector)]) = {

    println("\nSome Predictions from Test Data:\n")

    displayData.take(10).foreach(x => {
      println("---------------------------------------------------------------")
      println(s"Text = ${x._3}")
      println("Actual Label = " + (if (x._1 == 1) "positive" else if (x._1 == 2) "neutral" else "negative"))
      println("Predicted Label = " + (if (x._2 == 1) "positive" else if (x._2 == 2) "neutral" else "negative\n"))
      println(f"Predicted Probabilities : \n Negative   : ${x._4.toArray(0)}%2.2f \n Positive   : ${x._4.toArray(1)}%2.2f \n Neutral    : ${x._4.toArray(2)}%2.2f")
      println("----------------------------------------------------------------\n\n")
    })
  }
}
