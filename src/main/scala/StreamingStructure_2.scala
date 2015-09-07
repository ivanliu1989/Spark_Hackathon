package utilClasses

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ivanliu
 */
object StreamingStructure_2 {

  def main(args: Array[String]) {

//    if (args.length != 4) {
//      System.err.println(
//        "Usage: StreamingLogisticRegression <trainingDir> <testDir> <predBatchDuration> <trainBatchDuration>")
//      System.exit(1)
//    }
    // 1. Setup Parameters
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingLogisticRegression")
//    val sc = new StreamingContext(sparkConf, Seconds(args(2).toLong))

    val conf = new SparkConf().setMaster("local").setAppName("StreamingLogisticRegression")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    // 2. Streaming Outer Loop
    val train_trigger = if (args(3).toInt % (24 * 3600) == 0) true else false
    val trainingData = ssc.textFileStream(args(0)).map(LabeledPoint.parse)
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)
    val svmModel = if (train_trigger) {
      //train model
      println("Start to retrain models...")
    } else {
      //read model from disk
      println("Reading models...")
      val sparkConf = new SparkConf().setAppName("StreamingMachineLearning").setMaster("local[2]")
      val sc = new SparkContext(sparkConf)
      SVMModel.load(sc, "svmModelPath")
    }

    // 3. Streaming Inner Loop
//    val predict_lg = testData.map { point =>
//      val score = lgModel.predict(point.features)
//      (point.label, score)
//    }
//    val predict_svm = testData.map { point =>
//      val score = svmModel.predict(point.features)
//      (point.label, score)
//    }

    // 4. Ensemble Predictions
//    val ensemble_pred = predict_lg.join(predict_svm).mapValues(r => if ((r(0) + r(1)) /  > 0.5) 1 else 0)

    // 5. Save data
//    println(predict_svm)

    ssc.start()
    ssc.awaitTermination()

  }
}