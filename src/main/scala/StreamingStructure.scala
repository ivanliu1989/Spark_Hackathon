package utilClasses

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD, LogisticRegressionWithSGD }
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.streaming.{ Seconds, StreamingContext }

/**
 * @author ivanliu
 */
object StreamingStructure {

  val checkpoint_dir = "./checkpoint"

  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println(
        "Usage: StreamingLogisticRegression <trainingDir> <testDir> <predBatchDuration> <trainBatchDuration>")
      System.exit(1)
    }
    // 1. Setup Parameters
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingLogisticRegression")
    val ssc = StreamingContext.getOrCreate(checkpoint_dir, () => { CreateStreamingContext(sparkConf, args(2).toInt, args(3).toInt, args(0), args(1), 0) })

    ssc.start()
    ssc.awaitTermination()

  }

  // Streaming Context
  def CreateStreamingContext(sparkConf: SparkConf, interval: Int, train_interval: Int, train_path: String, test_path: String, windowLength: Int = 0): StreamingContext = {

    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    ssc.checkpoint(checkpoint_dir)

    // 2. Streaming Outer Loop
    val train_trigger = if (train_interval % (24 * 3600) == 0) true else false
    val trainingData = ssc.textFileStream(train_path).map(LabeledPoint.parse)
    val testData = ssc.textFileStream(test_path).map(LabeledPoint.parse)
    val lgModel, svmModel = if (train_trigger) {
      //train model
    } else {
      //read model from disk
    }

    // 3. Streaming Inner Loop
    val predict_lg = testData.map { point =>
      val score = lgModel.predict(point.features)
      (point.label, score)
    }
    val predict_svm = testData.map { point =>
      val score = svmModel.predict(point.features)
      (point.label, score)
    }

    // 4. Ensemble Predictions
    val ensemble_pred = predict_lg.join(predict_svm).mapValues(r => if ((r(0) + r(1)) / 2 > 0.5) 1 else 0)

    // 5. Save data

    ssc
  }
}