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

/**
 * @author ivanliu
 */
class utility {

  // 1. Calculate Date Difference
  def diff_days(s1: String, s2: String) = {
    val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date_unit = 1.15741e-8
    val date1 = date_format.parse(s1)
    val date2 = date_format.parse(s2)
    val delta: Long = date1.getTime() - date2.getTime()
    (delta * date_unit).toInt
  }

  // 2. Sparse Datasets & Convert to LabelPoint Format
  def generate_features(offers_df: String, trainHist_df: String, transactions_df: String) = {

    

  }

  // 3. Train the model

  // 4. Make Prediction

}
