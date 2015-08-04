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
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: StreamingLogisticRegression <trainingDir> <testDir> <predBatchDuration> <trainBatchDuration>")
      System.exit(1)
    }
    // 1. Setup Parameters
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingLogisticRegression")
    val sc = new StreamingContext(sparkConf, Seconds(args(2).toLong))

    val conf = new SparkConf().setMaster("local").setAppName("StreamingLogisticRegression")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    // 2. Steaming Outer Loop
    val train_trigger = if (args(3).toInt % (24*3600) ==0) true else false
    val trainingData = ssc.textFileStream(args(0)).map(LabeledPoint.parse)
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)
    val lgModel, svmModel = if(train_trigger) {} else {}

    lgModel.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    svmModel.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()

  }
}