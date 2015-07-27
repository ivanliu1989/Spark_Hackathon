import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


/**
 * @author ivanliu
 */
object StreamingMachineLearning {
  
  val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {
    
    //    define Spark Context
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    //Load and Check the data
    val offers = sc.textFile("../data/offers").map(_.split(","))
    val testHist = sc.textFile("../data/testHistory")
    val trainHist = sc.textFile("../data/trainHistory")
    val transactions = sc.textFile("../data/transactions")
    print(offers.take(100))
    print("I am Ivan")

    
  }
  
}