import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
 * @author ivanliu
 */
object StreamingMachineLearning {

  def main(args: Array[String]) {

    // 1.Define Spark Context
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // 1.1 Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    // 1.2 Load and Check the data
    val offers_df = sc.textFile("../data/offers").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(","))
    val testHist_df = sc.textFile("../data/testHistory").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(","))
    val trainHist_df = sc.textFile("../data/trainHistory").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(","))
    val transactions_df = sc.textFile("../data/transactions").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(","))
    // val transactions_df = sc.textFile("../data/transactions").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(",")).sample(false, fraction = 0.0001, seed = 123)

    // 1.3 Get all categories and comps on offer in a dict
    val offer_cat = offers_df.map(r => r(1)).collect()
    val offer_comp = offers_df.map(r => r(3)).collect()
    
    // 2. Reduce datasets - only write when if category in offers dict
    val transactions_df_filtered =  transactions_df.filter(r=>{offer_cat.contains(r(3)) || offer_comp.contains(r(4))}) //349655789 | 15349956 | 27764694 
    
    // 3. Feature Generation/Engineering
    // 3.1 keep a dictionary with the offerdata
    val offers_dict = offers_df.map(r => (r(0),r))
    // 3.2 keep two dictionaries with the shopper id's from test and train
    // val testHist_dict = testHist_df.map(r => ((r(0),r(1)),r))
    val trainHist_dict = trainHist_df.map(r => ((r(0),r(1)),r))
    val transactions_dict = transactions_df_filtered.map(r => ((r(0),r(1)),r))
    // transactions_dict.values.map(r=>r(3)).take(10)
    val main_data = trainHist_dict.join(transactions_dict).values.map(v=>v._1 ++ v._2)
    val main_data_2 = main_data.map(r => (r(2),r)).join(offers_dict).values.map(v=>v._1 ++ v._2)
  }

}


    // SQL Table offers
//    case class offers(offer: String, category: String, quantity: Int, Company: String, offervalue: Double, brand: String)
//    val offers_data = offers_df.map(r => offers(r(0), r(1), r(2).toInt, r(3), r(4).toDouble, r(5))).toDF()
//    
//    offers_data.registerTempTable("offers_table")

    // SQL Table testHist
//    case class testHist(id: String, chain: Int, offer: String, market: String, offerdate: String)
//    val testHist_data = testHist_dfmap(r => testHist(r(0), r(1).toInt, r(2), r(3), r(4))).toDF()
//
//    testHist_data.registerTempTable("testHist_table")

    // SQL Table trainHist
//    case class trainHist(id: String, chain: Int, offer: String, market: String, repeattrips: Int, repeater: String, offerdate: String)
//    val trainHist_data = trainHist_df.map(r => trainHist(r(0), r(1).toInt, r(2), r(3), r(4).toInt, r(5), r(6))).toDF()
//
//    trainHist_data.registerTempTable("trainHist_table")

    // SQL Table transactions
//    case class transactions(id: String, chain: Int, dept: String, category: String, company: String, brand: String, date: String, productsize: String, productmeasure: String, purchasequantity: Int, purchaseamount: Double)
//    val transactions_data = transactions_df.map(r => transactions(r(0), r(1).toInt, r(2), r(3), r(4), r(5), r(6), r(6), r(7), r(8), r(9).toInt, r(10).toDouble)).toDF()
//
//    transactions_data.registerTempTable("transactions_table")

    // SQL Query
    // sqlContext.sql("select offerdate from offers_table").collect().foreach(println)
    