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
    //val transactions_df = sc.textFile("../data/transactions").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(",")).sample(false, fraction = 0.01, seed = 123)

    // 1.3 Get all categories and comps on offer in a dict
    val offer_cat = offers_df.map(r => r(1)).collect()
    val offer_comp = offers_df.map(r => r(3)).collect()

    // 2. Reduce datasets - only write when if category in offers dict
    val transactions_df_filtered = transactions_df.filter(r => { offer_cat.contains(r(3)) || offer_comp.contains(r(4)) }) //349655789 | 15349956 | 27764694 

    // 3. Feature Generation/Engineering
    // 3.1 keep a dictionary with the offerdata
    val offers_dict = offers_df.map(r => (r(0), r))
    // 3.2 keep two dictionaries with the shopper id's from test and train
    // val testHist_dict = testHist_df.map(r => ((r(0),r(1)),r))
    val trainHist_dict = trainHist_df.map(r => (r(2), r))
    val transactions_dict = transactions_df_filtered.map(r => ((r(0), r(1)), r)) //,r(3),r(4),r(5)
    // Features: 0.id, 1.chain, 2.dept, 3.category, 4.company, 5.brand, 6.date, 7.productsize, 8.productmeasure, 9.purchasequantity, 10.purchaseamount
    val train_offer = trainHist_dict.join(offers_dict).values.map(v => v._1 ++ v._2).map(r => ((r(0), r(1)), Array(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(8), r(9), r(10), r(11), r(12)))) //,r(8),r(10),r(12)
    // Features: 0.id, 1.chain, 2.offer, 3.market, 4.repeattrips, 5.repeater, 6.offerdate, 7.category, 8.quantity, 9.company, 10.offervalue, 11.brand
    val main_data = train_offer.fullOuterJoin(transactions_dict).values.filter(v => (!v._1.isEmpty && !v._2.isEmpty)).map(r => {
      val a = r._1.toArray
      val b = r._2.toArray
      a(0) ++ b(0)
    }).map(r => Array(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(14), r(15), r(16), r(17), r(18), r(19), r(20), r(21), r(22)))
    /* Features: 0.id, 1.chain, 2.offer, 3.market, 4.repeattrips, 5.repeater, 6.offerdate, 7.o_category, 8.quantity, 9.o_company, 10.offervalue, 11.o_brand,   
       Features: 12.dept, 13.t_category, 14.t_company, 15.t_brand, 16.date, 17.productsize, 18.productmeasure, 19.purchasequantity, 20.purchaseamount */

    // 3.3 Filter transactions happened after offers
    val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date_unit = 1.15741e-8
    def diff_days(s1: String, s2: String) = {
      val date1 = date_format.parse(s1)
      val date2 = date_format.parse(s2)
      val delta: Long = date1.getTime() - date2.getTime()
      (delta * date_unit).toInt
    }
    val main_data_filter = main_data.filter(r => { diff_days(r(6), r(16)) > 0 })

    // 3.4 Generate six new features

    val main_data_nFeat = main_data_filter.map(r => Array(r(0), r(1), r(2), r(3), r(4), r(5), r(8), r(10)) ++ {
      val h_company = r(9)
      val h_category = r(7)
      val h_brand = r(11)
      val t_company = r(14)
      val t_category = r(13)
      val t_brand = r(15)

      // Overall
      val has_bought_company = if (h_company == t_company) 1 else 0
      val has_bought_company_q = if (h_company == t_company) 1 else r(19)
      val has_bought_company_a = if (h_company == t_company) 1 else r(20)

      val has_bought_category = if (h_category == t_category) 1 else 0
      val has_bought_category_q = if (h_category == t_category) 1 else r(19)
      val has_bought_category_a = if (h_category == t_category) 1 else r(20)

      val has_bought_brand = if (h_brand == t_brand) 1 else 0
      val has_bought_brand_q = if (h_brand == t_brand) 1 else r(19)
      val has_bought_brand_a = if (h_brand == t_brand) 1 else r(20)

      val has_bought_brand_company_category = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1) 1 else 0
      val has_bought_brand_company_category_q = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1) r(19) else 0
      val has_bought_brand_company_category_a = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1) r(20) else 0

      val has_bought_brand_category = if (has_bought_category == 1 && has_bought_brand == 1) 1 else 0
      val has_bought_brand_category_q = if (has_bought_category == 1 && has_bought_brand == 1) r(19) else 0
      val has_bought_brand_category_a = if (has_bought_category == 1 && has_bought_brand == 1) r(20) else 0

      val has_bought_brand_company = if (has_bought_company == 1 && has_bought_brand == 1) 1 else 0
      val has_bought_brand_company_q = if (has_bought_company == 1 && has_bought_brand == 1) r(19) else 0
      val has_bought_brand_company_a = if (has_bought_company == 1 && has_bought_brand == 1) r(20) else 0

      // 30 Days
      val has_bought_company_30 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 30) 1 else 0
      val has_bought_company_q_30 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 30) r(19) else 0
      val has_bought_company_a_30 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 30) r(20) else 0

      val has_bought_category_30 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 30) 1 else 0
      val has_bought_category_q_30 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 30) r(19) else 0
      val has_bought_category_a_30 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 30) r(20) else 0

      val has_bought_brand_30 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1 else 0
      val has_bought_brand_q_30 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19) else 0
      val has_bought_brand_a_30 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20) else 0

      val has_bought_brand_company_category_30 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1 else 0
      val has_bought_brand_company_category_q_30 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19) else 0
      val has_bought_brand_company_category_a_30 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20) else 0

      val has_bought_brand_category_30 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1 else 0
      val has_bought_brand_category_q_30 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19) else 0
      val has_bought_brand_category_a_30 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20) else 0

      val has_bought_brand_company_30 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1 else 0
      val has_bought_brand_company_q_30 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19) else 0
      val has_bought_brand_company_a_30 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20) else 0

      // 60 Days
      val has_bought_company_60 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 60) 1 else 0
      val has_bought_company_q_60 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 60) r(19) else 0
      val has_bought_company_a_60 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 60) r(20) else 0

      val has_bought_category_60 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 60) 1 else 0
      val has_bought_category_q_60 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 60) r(19) else 0
      val has_bought_category_a_60 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 60) r(20) else 0

      val has_bought_brand_60 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1 else 0
      val has_bought_brand_q_60 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19) else 0
      val has_bought_brand_a_60 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20) else 0

      val has_bought_brand_company_category_60 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1 else 0
      val has_bought_brand_company_category_q_60 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19) else 0
      val has_bought_brand_company_category_a_60 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20) else 0

      val has_bought_brand_category_60 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1 else 0
      val has_bought_brand_category_q_60 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19) else 0
      val has_bought_brand_category_a_60 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20) else 0

      val has_bought_brand_company_60 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1 else 0
      val has_bought_brand_company_q_60 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19) else 0
      val has_bought_brand_company_a_60 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20) else 0

      // 90 Days
      val has_bought_company_90 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 90) 1 else 0
      val has_bought_company_q_90 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 90) r(19) else 0
      val has_bought_company_a_90 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 90) r(20) else 0

      val has_bought_category_90 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 90) 1 else 0
      val has_bought_category_q_90 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 90) r(19) else 0
      val has_bought_category_a_90 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 90) r(20) else 0

      val has_bought_brand_90 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1 else 0
      val has_bought_brand_q_90 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19) else 0
      val has_bought_brand_a_90 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20) else 0

      val has_bought_brand_company_category_90 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1 else 0
      val has_bought_brand_company_category_q_90 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19) else 0
      val has_bought_brand_company_category_a_90 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20) else 0

      val has_bought_brand_category_90 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1 else 0
      val has_bought_brand_category_q_90 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19) else 0
      val has_bought_brand_category_a_90 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20) else 0

      val has_bought_brand_company_90 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1 else 0
      val has_bought_brand_company_q_90 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19) else 0
      val has_bought_brand_company_a_90 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20) else 0

      // 180 Days
      val has_bought_company_180 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 180) 1 else 0
      val has_bought_company_q_180 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 180) r(19) else 0
      val has_bought_company_a_180 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 180) r(20) else 0

      val has_bought_category_180 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 180) 1 else 0
      val has_bought_category_q_180 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 180) r(19) else 0
      val has_bought_category_a_180 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 180) r(20) else 0

      val has_bought_brand_180 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1 else 0
      val has_bought_brand_q_180 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19) else 0
      val has_bought_brand_a_180 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20) else 0

      val has_bought_brand_company_category_180 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1 else 0
      val has_bought_brand_company_category_q_180 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19) else 0
      val has_bought_brand_company_category_a_180 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20) else 0

      val has_bought_brand_category_180 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1 else 0
      val has_bought_brand_category_q_180 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19) else 0
      val has_bought_brand_category_a_180 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20) else 0

      val has_bought_brand_company_180 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1 else 0
      val has_bought_brand_company_q_180 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19) else 0
      val has_bought_brand_company_a_180 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20) else 0

      Array(has_bought_company, has_bought_company_q, has_bought_company_a,
        has_bought_category, has_bought_category_q, has_bought_category_a,
        has_bought_brand, has_bought_brand_q, has_bought_brand_a,
        has_bought_brand_company_category, has_bought_brand_company_category_q, has_bought_brand_company_category_a,
        has_bought_brand_category, has_bought_brand_category_q, has_bought_brand_category_a,
        has_bought_brand_company, has_bought_brand_company_q, has_bought_brand_company_a,

        has_bought_company_30, has_bought_company_q_30, has_bought_company_a_30,
        has_bought_category_30, has_bought_category_q_30, has_bought_category_a_30,
        has_bought_brand_30, has_bought_brand_q_30, has_bought_brand_a_30,
        has_bought_brand_company_category_30, has_bought_brand_company_category_q_30, has_bought_brand_company_category_a_30,
        has_bought_brand_category_30, has_bought_brand_category_q_30, has_bought_brand_category_a_30,
        has_bought_brand_company_30, has_bought_brand_company_q_30, has_bought_brand_company_a_30,

        has_bought_company_60, has_bought_company_q_60, has_bought_company_a_60,
        has_bought_category_60, has_bought_category_q_60, has_bought_category_a_60,
        has_bought_brand_60, has_bought_brand_q_60, has_bought_brand_a_60,
        has_bought_brand_company_category_60, has_bought_brand_company_category_q_60, has_bought_brand_company_category_a_60,
        has_bought_brand_category_60, has_bought_brand_category_q_60, has_bought_brand_category_a_60,
        has_bought_brand_company_60, has_bought_brand_company_q_60, has_bought_brand_company_a_60,

        has_bought_company_90, has_bought_company_q_90, has_bought_company_a_90,
        has_bought_category_90, has_bought_category_q_90, has_bought_category_a_90,
        has_bought_brand_90, has_bought_brand_q_90, has_bought_brand_a_90,
        has_bought_brand_company_category_90, has_bought_brand_company_category_q_90, has_bought_brand_company_category_a_90,
        has_bought_brand_category_90, has_bought_brand_category_q_90, has_bought_brand_category_a_90,
        has_bought_brand_company_90, has_bought_brand_company_q_90, has_bought_brand_company_a_90,

        has_bought_company_180, has_bought_company_q_180, has_bought_company_a_180,
        has_bought_category_180, has_bought_category_q_180, has_bought_category_a_180,
        has_bought_brand_180, has_bought_brand_q_180, has_bought_brand_a_180,
        has_bought_brand_company_category_180, has_bought_brand_company_category_q_180, has_bought_brand_company_category_a_180,
        has_bought_brand_category_180, has_bought_brand_category_q_180, has_bought_brand_category_a_180,
        has_bought_brand_company_180, has_bought_brand_company_q_180, has_bought_brand_company_a_180)
    })

    // 3.5 Aggregate Transactions and generate new features
    // reduceByKey => Key(trainHist-2~11) Attributes(Transactions-12~20)
    /* Features: 0.id, 1.chain, 2.offer, 3.market, 4.repeattrips, 5.repeater, 6.quantity, 7.offervalue    
       Features: 8~79.(72 features) 
       Removed Features: 6.offerdate, 7.o_category, 9.o_company, 11.o_brand, 12.dept, 13.productsize, 14.productmeasure 
       
       Cat: 0, 1, 2, 3
       Num: 4,6~79
       Target: 5  
     */
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
    
