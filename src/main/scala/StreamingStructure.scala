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
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ivanliu
 */
object StreamingStructure {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: StreamingLogisticRegression <trainingDir> <testDir> <batchDuration> <numFeatures>")
      System.exit(1)
    }
    
    val conf = new SparkConf().setMaster("local").setAppName("StreamingLogisticRegression")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

  }
}