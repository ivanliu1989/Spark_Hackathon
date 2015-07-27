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
    
    // SQL Table offers
    case class offers(offer: String, category: String, quantity: Int, Company: String, offervalue: Double, brand: String)
    val offers_data = offers_df.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
                               .map(_.split(","))
                               .map(r=>offers(r(0),r(1),r(2).toInt,r(3),r(4).toDouble,r(5)))//.toDF()
                               
    offers_data.registerTempTable("offers_table") 
    
    // SQL Table testHist
    case class testHist(id: String, chain: String, offer: String, market: String, offerdate: String)
    val testHist_data = testHist_df.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
                                   .map(_.split(","))
                                   .map(r=>testHist(r(0),r(1),r(2),r(3),r(4)))//.toDF()
                               
    testHist_data.registerTempTable("testHist_table") 
    
    // SQL Table trainHist
    case class trainHist(id: String, chain: String, offer: String, market: String, repeattrips: Int, repeater: String, offerdate: String)
    val trainHist_data = trainHist_df.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
                                   .map(_.split(","))
                                   .map(r=>trainHist(r(0),r(1),r(2),r(3),r(4).toInt,r(5),r(6)))//.toDF()
                               
    trainHist_data.registerTempTable("trainHist_table") 
    
    // SQL Table transactions
    case class transactions(id: String, chain: String, offer: String, market: String, repeattrips: Int, repeater: String, offerdate: String)
    val transactions_data = transactions_df.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
                                           .map(_.split(","))
                                           .map(r=>transactions(r(0),r(1),r(2),r(3),r(4).toInt,r(5),r(6)))//.toDF()
                               
    transactions_data.registerTempTable("transactions_table") 
  }
  
}