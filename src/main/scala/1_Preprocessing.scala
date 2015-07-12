import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ivanliu on 12/07/15.
 */

object Preprocessing {

  def main(args: Array[String]): Unit = {
    //    define Spark Context
    val conf = new SparkConf().setAppName("Streaming Example").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val offers = sc.textFile("../data/offers").map(_.split(","))
    val testHist = sc.textFile("../data/testHistory")
    val trainHist = sc.textFile("../data/trainHistory")
    val transactions = sc.textFile("../data/transactions")
//    print(transactions.count())
    offers.foreach(println)
  }
}

