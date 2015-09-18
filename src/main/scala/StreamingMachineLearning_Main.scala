package utilClasses

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utilClasses.modelPredict.predict
import utilClasses.modelTraining.train
import java.nio.file.{Paths, Files}
/**
 * @author ivanliu
 */
object StreamingMachineLearning_Main {
  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println(
        "Usage: StreamingLogisticRegression <trainingDir> <testDir> <BatchDuration> <trainHour>")
      System.exit(1)
    }
    
    /* 1. Setup Parameters */
    val conf = new SparkConf().setMaster("local").setAppName("StreamingLogisticRegression")
    conf.set("spark.driver.allowMultipleContexts","true")
//     val ssc = new StreamingContext(conf, Seconds(300))
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    val predictions = ssc.textFileStream("models/predictions/")
    
    val now = new Date   
    val dateFormatter = new SimpleDateFormat("y-M-d")
    val hourFormatter = new SimpleDateFormat("H")
    // val df = getDateInstance(LONG, Locale.US)  
    
    val model_path = "models/logistic/lgModel_"
    val file_name = model_path + dateFormatter.format(now)
    val hour_trigger = hourFormatter.format(now).toInt
    println(file_name)
    println(hour_trigger)
    
    
    /* 2. Start to training data */
    val offer_path = "data/offers"
    val transaction_path = "data/transactions_sample/"
    val train_path = args(0).toString
    val test_path = args(1).toString
    val tHour = args(3).toInt
//    val train_path = "../data/trainHistory"
//    val test_path = "../data/testHistory"
//    val tHour = 21


    /* 2.1 Training or Predicting */
    //Time trigger to re-train the model AND/OR no existing model has been found
    predictions.foreachRDD(r => if ((hour_trigger == tHour & !Files.exists(Paths.get(file_name))) || !Files.exists(Paths.get(file_name))){ 
//      val svmModel, lgModel = train(offer_path, train_path, test_path, transaction_path, file_name)
      val lgModel = train(offer_path, train_path, test_path, transaction_path, file_name)
    }
    else{
      val pred_lg = predict(offer_path, test_path, transaction_path,file_name)
    })
    
    ssc.start()
    ssc.awaitTermination()

  }
}