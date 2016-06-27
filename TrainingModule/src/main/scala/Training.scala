import Model._
import Utils._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Training {

  def createTrainingLabel(sc: SparkContext) = {
    val trainData = sc.textFile(getClass.getClassLoader.getResource("TrainingData.csv").getPath)
    val parsedData = parseFile(trainData)

    val trainingVector = parsedData.map(x => (x._1, tf.transform(cleanseData(x._2))))
    trainingVector.map(l => new LabeledPoint(l._1.toDouble, l._2))
  }

  def modelTraining(labelPoint: RDD[(LabeledPoint)]) = {
    println("\n\n********* Training The Model **********\n\n")
    trainModel(labelPoint)
  }
}