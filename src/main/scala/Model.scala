import java.util.Date

import Utils._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import twitter4j.GeoLocation

object Model {

  def trainModel(labelPoint: RDD[(LabeledPoint)]) = {
    time {
      NaiveBayes.train(labelPoint, 1.0)
    }
  }

  def convertActualTwitterData(actualData: DStream[(String, Date, GeoLocation)]) = {
    actualData.filter(x => !x._1.isEmpty && !x._1.equals("")).map(t => (tf.transform(cleanseData(t._1)), t._1, t._2, t._3))
  }

  def convertUserTwitterData(actualData: RDD[(String)]) = {
    actualData.filter(x => !x.isEmpty && !x.equals("")).map(t => (tf.transform(cleanseData(t)), t))
  }

  def predictLabel(test_label: DStream[(Vector, String, Date, GeoLocation)], model: NaiveBayesModel) = {
    time {
      test_label.filter(x => !x._2.isEmpty && !x._2.equals("")).map(p => {
        val text = p._2
        val features = p._1
        val predicted_label = model.predict(features)
        val predicted_prob = model.predictProbabilities(features)
        (predicted_label, text, predicted_prob, p._3, p._4)
      })
    }
  }

  def predictUserLabel(test_label: RDD[(Vector, String)], model: NaiveBayesModel) = {
    time {
      test_label.filter(x => !x._2.isEmpty && !x._2.equals("")).map(p => {
        val text = p._2
        val features = p._1
        val predicted_label = model.predict(features)
        val predicted_prob = model.predictProbabilities(features)
        (predicted_label, text, predicted_prob)
      })
    }
  }
}
