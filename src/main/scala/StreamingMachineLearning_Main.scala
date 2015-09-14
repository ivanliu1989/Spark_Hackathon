package utilClasses

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utilClasses.modelTraining.{train}
import utilClasses.modelPredict.{predict}


/**
 * @author ivanliu
 */
object StreamingMachineLearning_Main {
  def main(args: Array[String]) {

//    if (args.length != 4) {
//      System.err.println(
//        "Usage: StreamingLogisticRegression <trainingDir> <testDir> <predBatchDuration> <trainBatchDuration>")
//      System.exit(1)
//    }
    // 1. Setup Parameters
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingLogisticRegression")
//    val sc = new StreamingContext(sparkConf, Seconds(args(2).toLong))
    val i = 0
    val conf = new SparkConf().setMaster("local").setAppName("StreamingLogisticRegression")
    conf.set("spark.driver.allowMultipleContexts","true")
//    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))
    val ssc = new StreamingContext(conf, Seconds(300))

    // 2. Streaming Outer Loop
//    val train_trigger = if (args(3).toInt % (24 * 3600) == 0) true else false
//    val trainingData = ssc.textFileStream(args(0))
//    val testData = ssc.textFileStream(args(1))

/* Start to training data */
    val offer_path = "../data/offers"
    val train_path = "../data/trainHistory"
    val test_path = "../data/testHistory"
    val transaction_path = "../data/transactions"
    val model_path = "lgModelPath"//"/models/logistic/lgModel"
//    val svmModel, lgModel = train(offer_path, train_path, test_path, transaction_path)
    val pred_lg = predict(offer_path, test_path, transaction_path,model_path)
    

    ssc.start()
    ssc.awaitTermination()

  }
}