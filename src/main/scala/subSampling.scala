package utilClasses

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utilClasses.utility.diff_days

/**
 * @author ivanliu
 */
object subSampling {
   def main(args: Array[String]) {
     
    /* 1.1 Define Spark Context */
    val sparkConf = new SparkConf().setAppName("SubSamplingTransactionData").setMaster("local[2]")
    sparkConf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(sparkConf)
    
    /* 1.2 Load and Sub Sample the data */
    val transactions_df = sc.textFile("../data/transactions").sample(false, fraction = 0.01, seed = 19890624)
    transactions_df.saveAsTextFile("../data/transactions_sample")
   }
}