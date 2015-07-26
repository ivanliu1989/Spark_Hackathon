import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ivanliu on 12/07/15.
 */

object Preprocessing {

  val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {
    //    define Spark Context
    val conf = new SparkConf().setAppName("Streaming Example").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //Load and Check the data
    val offers = sc.textFile("../data/offers").map(_.split(","))
    val testHist = sc.textFile("../data/testHistory")
    val trainHist = sc.textFile("../data/trainHistory")
    val transactions = sc.textFile("../data/transactions")
    print(offers.take(100))
    print("I am Ivan")
//    val tran_sample = transactions.sample(false, fraction = 0.00001, seed = 123).cache()
//    print(tran_sample.count())
    //    tran_sample.foreach(println)
  }

  def reduce_data(): Unit={

  }
  def diff_days(s1: String, s2:String): Int={
    val date1 = date_format.parse(s1)
    val date2 = date_format.parse(s2)

  }
  def generate_features(): Unit={

  }
  //  def parsePoint(line:String): Unit ={
  //    /*Converts a comma separated unicode string into a `LabeledPoint`.
  //
//    Args:
//        line (unicode): Comma separated unicode string where the first element is the label and the
//            remaining elements are features.
//
//    Returns:
//        LabeledPoint: The line is converted into a `LabeledPoint`, which consists of a label and
//            features.*/
//
//    val lines = LabeledPoint(line.split(",")(0), line.split(",")(-0))
//  }
}

