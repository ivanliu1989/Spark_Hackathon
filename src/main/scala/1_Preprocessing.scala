import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ivanliu on 12/07/15.
 */

object Preprocessing {

  def main(args: Array[String]): Unit = {
    //    define Spark Context
    val conf = new SparkConf().setAppName("Streaming Example").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //Load and Check the data
    val offers = sc.textFile("../data/offers").map(_.split(","))
    val testHist = sc.textFile("../data/testHistory")
    val trainHist = sc.textFile("../data/trainHistory")
    val transactions = sc.textFile("../data/transactions")
    val tran_sample = transactions.sample(false, fraction = 0.00001, seed = 123)
    print(tran_sample.count())
    //    tran_sample.foreach(println)
  }

  def parsePoint(line:String): Unit ={
    /*Converts a comma separated unicode string into a `LabeledPoint`.

    Args:
        line (unicode): Comma separated unicode string where the first element is the label and the
            remaining elements are features.

    Returns:
        LabeledPoint: The line is converted into a `LabeledPoint`, which consists of a label and
            features.*/
    

  }
}

