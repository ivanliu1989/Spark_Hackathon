package utilClasses

import org.apache.spark.{SparkConf, SparkContext}

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
    val transactions_df = sc.textFile("../data/transactions").sample(false, fraction = 0.001, seed = 19890624)
    transactions_df.saveAsTextFile("../data/transactions_sample")
   }
}