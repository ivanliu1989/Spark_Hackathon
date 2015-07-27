import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


/**
 * @author ivanliu
 */
object StreamingMachineLearning {
  
  //val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]) {
    
    // Define Spark Context
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._
    
    //Load and Check the data
    val offers_df = sc.textFile("../data/offers")
    val testHist_df = sc.textFile("../data/testHistory")
    val trainHist_df = sc.textFile("../data/trainHistory")
    val transactions_df = sc.textFile("../data/transactions")
    
    // case Class
    case class offers(offer: String, category: String, quantity: Int, Company: String, offervalue: Double, brand: String)
    val offers_data = offers_df.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
                               .map(_.split(","))
                               .map(r=>offers(r(0),r(1),r(2).toInt,r(3),r(4).toDouble,r(5))).toDF()
                               
    offers_data.registerTempTable("offers_table")                      
  }
  
}