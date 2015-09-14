package utilClasses

import org.apache.spark.mllib.classification.{LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utilClasses.utility.diff_days

/**
 * @author ivanliu
 */
object modelPredict {
  def predict(offers_path: String, test_path: String, transaction_path: String, model_path: String ) {
    // 1.Define Spark Context
    val sparkConf = new SparkConf().setAppName("StreamingMachineLearning").setMaster("local[2]")
    sparkConf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(sparkConf)
    println("Start Loading Datasets ...")
    
    // 1.2 Load and Check the data
    val offers_df = sc.textFile(offers_path).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(","))
    val testHist_df = sc.textFile(test_path).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(","))
//    val transactions_df = sc.textFile(transaction_path).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(","))
    val transactions_df = sc.textFile(transaction_path).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(",")).sample(false, fraction = 0.01, seed = 123)

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
    val testHist_dict = testHist_df.map(r => (r(2), r))
    val transactions_dict = transactions_df_filtered.map(r => ((r(0), r(1)), r)) //,r(3),r(4),r(5)
    // Features: 0.id, 1.chain, 2.dept, 3.category, 4.company, 5.brand, 6.date, 7.productsize, 8.productmeasure, 9.purchasequantity, 10.purchaseamount
    val test_offer = testHist_dict.join(offers_dict).values.map(v => v._1 ++ v._2).map(r => ((r(0), r(1)), Array(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(8), r(9), r(10), r(11), r(12)))) //,r(8),r(10),r(12)
    // Features: 0.id, 1.chain, 2.offer, 3.market, 4.repeattrips, 5.repeater, 6.offerdate, 7.category, 8.quantity, 9.company, 10.offervalue, 11.brand
    val main_data = test_offer.fullOuterJoin(transactions_dict).values.filter(v => (!v._1.isEmpty && !v._2.isEmpty)).map(r => {
      val a = r._1.toArray
      val b = r._2.toArray
      a(0) ++ b(0)
    }).map(r => Array(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(14), r(15), r(16), r(17), r(18), r(19), r(20), r(21), r(22)))
    /* Features: 0.id, 1.chain, 2.offer, 3.market, 4.repeattrips, 5.repeater, 6.offerdate, 7.o_category, 8.quantity, 9.o_company, 10.offervalue, 11.o_brand,   
       Features: 12.dept, 13.t_category, 14.t_company, 15.t_brand, 16.date, 17.productsize, 18.productmeasure, 19.purchasequantity, 20.purchaseamount */

    // 3.3 Filter transactions happened after offers
    val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date_unit = 1.15741e-8
    val main_data_filter = main_data.filter(r => { utility.diff_days(r(6), r(16)) > 0 })

    // 3.4 Generate 90 new features
    val main_data_nFeat = main_data_filter.map(r => Array(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble, r(4).toDouble, if (r(5) == "t") 1.0 else 0.0, r(8).toDouble, r(10).toDouble) ++ {
      val h_company = r(9)
      val h_category = r(7)
      val h_brand = r(11)
      val t_company = r(14)
      val t_category = r(13)
      val t_brand = r(15)

      // Overall
      val has_bought_company = if (h_company == t_company) 1.0 else 0
      val has_bought_company_q = if (h_company == t_company) r(19).toDouble else 0
      val has_bought_company_a = if (h_company == t_company) r(20).toDouble else 0

      val has_bought_category = if (h_category == t_category) 1.0 else 0
      val has_bought_category_q = if (h_category == t_category) r(19).toDouble else 0
      val has_bought_category_a = if (h_category == t_category) 1 else r(20).toDouble

      val has_bought_brand = if (h_brand == t_brand) 1.0 else 0
      val has_bought_brand_q = if (h_brand == t_brand) r(19).toDouble else 0
      val has_bought_brand_a = if (h_brand == t_brand) 1 else r(20).toDouble

      val has_bought_brand_company_category = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1) 1.0 else 0
      val has_bought_brand_company_category_q = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1) r(19).toDouble else 0
      val has_bought_brand_company_category_a = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1) r(20).toDouble else 0

      val has_bought_brand_category = if (has_bought_category == 1 && has_bought_brand == 1) 1.0 else 0
      val has_bought_brand_category_q = if (has_bought_category == 1 && has_bought_brand == 1) r(19).toDouble else 0
      val has_bought_brand_category_a = if (has_bought_category == 1 && has_bought_brand == 1) r(20).toDouble else 0

      val has_bought_brand_company = if (has_bought_company == 1 && has_bought_brand == 1) 1.0 else 0
      val has_bought_brand_company_q = if (has_bought_company == 1 && has_bought_brand == 1) r(19).toDouble else 0
      val has_bought_brand_company_a = if (has_bought_company == 1 && has_bought_brand == 1) r(20).toDouble else 0

      // 30 Days
      val has_bought_company_30 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 30) 1.0 else 0
      val has_bought_company_q_30 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 30) r(19).toDouble else 0
      val has_bought_company_a_30 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 30) r(20).toDouble else 0

      val has_bought_category_30 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 30) 1.0 else 0
      val has_bought_category_q_30 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 30) r(19).toDouble else 0
      val has_bought_category_a_30 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 30) r(20).toDouble else 0

      val has_bought_brand_30 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1.0 else 0
      val has_bought_brand_q_30 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19).toDouble else 0
      val has_bought_brand_a_30 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20).toDouble else 0

      val has_bought_brand_company_category_30 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1.0 else 0
      val has_bought_brand_company_category_q_30 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19).toDouble else 0
      val has_bought_brand_company_category_a_30 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20).toDouble else 0

      val has_bought_brand_category_30 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1.0 else 0
      val has_bought_brand_category_q_30 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19).toDouble else 0
      val has_bought_brand_category_a_30 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20).toDouble else 0

      val has_bought_brand_company_30 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) 1.0 else 0
      val has_bought_brand_company_q_30 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(19).toDouble else 0
      val has_bought_brand_company_a_30 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 30) r(20).toDouble else 0

      // 60 Days
      val has_bought_company_60 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 60) 1.0 else 0
      val has_bought_company_q_60 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 60) r(19).toDouble else 0
      val has_bought_company_a_60 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 60) r(20).toDouble else 0

      val has_bought_category_60 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 60) 1.0 else 0
      val has_bought_category_q_60 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 60) r(19).toDouble else 0
      val has_bought_category_a_60 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 60) r(20).toDouble else 0

      val has_bought_brand_60 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1.0 else 0
      val has_bought_brand_q_60 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19).toDouble else 0
      val has_bought_brand_a_60 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20).toDouble else 0

      val has_bought_brand_company_category_60 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1.0 else 0
      val has_bought_brand_company_category_q_60 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19).toDouble else 0
      val has_bought_brand_company_category_a_60 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20).toDouble else 0

      val has_bought_brand_category_60 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1.0 else 0
      val has_bought_brand_category_q_60 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19).toDouble else 0
      val has_bought_brand_category_a_60 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20).toDouble else 0

      val has_bought_brand_company_60 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) 1.0 else 0
      val has_bought_brand_company_q_60 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(19).toDouble else 0
      val has_bought_brand_company_a_60 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 60) r(20).toDouble else 0

      // 90 Days
      val has_bought_company_90 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 90) 1.0 else 0
      val has_bought_company_q_90 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 90) r(19).toDouble else 0
      val has_bought_company_a_90 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 90) r(20).toDouble else 0

      val has_bought_category_90 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 90) 1.0 else 0
      val has_bought_category_q_90 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 90) r(19).toDouble else 0
      val has_bought_category_a_90 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 90) r(20).toDouble else 0

      val has_bought_brand_90 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1.0 else 0
      val has_bought_brand_q_90 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19).toDouble else 0
      val has_bought_brand_a_90 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20).toDouble else 0

      val has_bought_brand_company_category_90 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1.0 else 0
      val has_bought_brand_company_category_q_90 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19).toDouble else 0
      val has_bought_brand_company_category_a_90 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20).toDouble else 0

      val has_bought_brand_category_90 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1.0 else 0
      val has_bought_brand_category_q_90 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19).toDouble else 0
      val has_bought_brand_category_a_90 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20).toDouble else 0

      val has_bought_brand_company_90 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) 1.0 else 0
      val has_bought_brand_company_q_90 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(19).toDouble else 0
      val has_bought_brand_company_a_90 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 90) r(20).toDouble else 0

      // 180 Days
      val has_bought_company_180 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 180) 1.0 else 0
      val has_bought_company_q_180 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 180) r(19).toDouble else 0
      val has_bought_company_a_180 = if (has_bought_company == 1 && diff_days(r(6), r(16)) < 180) r(20).toDouble else 0

      val has_bought_category_180 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 180) 1.0 else 0
      val has_bought_category_q_180 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 180) r(19).toDouble else 0
      val has_bought_category_a_180 = if (has_bought_category == 1 && diff_days(r(6), r(16)) < 180) r(20).toDouble else 0

      val has_bought_brand_180 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1.0 else 0
      val has_bought_brand_q_180 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19).toDouble else 0
      val has_bought_brand_a_180 = if (has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20).toDouble else 0

      val has_bought_brand_company_category_180 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1.0 else 0
      val has_bought_brand_company_category_q_180 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19).toDouble else 0
      val has_bought_brand_company_category_a_180 = if (has_bought_company == 1 && has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20).toDouble else 0

      val has_bought_brand_category_180 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1.0 else 0
      val has_bought_brand_category_q_180 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19).toDouble else 0
      val has_bought_brand_category_a_180 = if (has_bought_category == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20).toDouble else 0

      val has_bought_brand_company_180 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) 1.0 else 0
      val has_bought_brand_company_q_180 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(19).toDouble else 0
      val has_bought_brand_company_a_180 = if (has_bought_company == 1 && has_bought_brand == 1 && diff_days(r(6), r(16)) < 180) r(20).toDouble else 0

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
       Features: 8~98.(72 features) 
       Removed Features: 6.offerdate, 7.o_category, 9.o_company, 11.o_brand, 12.dept, 13.productsize, 14.productmeasure 
       
       Cat: 0, 1, 2, 3
       Num: 4,6~98
       Target: 5  
     */
    val main_data_agg = main_data_nFeat.map(r => ((r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7)),
      Array(r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17), r(18), r(19), r(20),
        r(21), r(22), r(23), r(24), r(25), r(26), r(27), r(28), r(29), r(30),
        r(31), r(32), r(33), r(34), r(35), r(36), r(37), r(38), r(39), r(40),
        r(41), r(42), r(43), r(44), r(45), r(46), r(47), r(48), r(49), r(50),
        r(51), r(52), r(53), r(54), r(55), r(56), r(57), r(58), r(59), r(60),
        r(61), r(62), r(63), r(64), r(65), r(66), r(67), r(68), r(69), r(70),
        r(71), r(72), r(73), r(74), r(75), r(76), r(77), r(78), r(79), r(80),
        r(81), r(82), r(83), r(84), r(85), r(86), r(87), r(88), r(89), r(90),
        r(91), r(92), r(93), r(94), r(95), r(96), r(97)))).mapValues { r =>
      val array = r.toSeq.toArray
      array.map(_.asInstanceOf[Double])
    }.reduceByKey((x, y) => Array(x(0) + y(0), x(1) + y(1), x(2) + y(2), x(3) + y(3), x(4) + y(4), x(5) + y(5), x(6) + y(6), x(7) + y(7), x(8) + y(8), x(9) + y(9),
      x(10) + y(10), x(11) + y(11), x(12) + y(12), x(13) + y(13), x(14) + y(14), x(15) + y(15), x(16) + y(16), x(17) + y(17), x(18) + y(18), x(19) + y(19),
      x(20) + y(20), x(21) + y(21), x(22) + y(22), x(23) + y(23), x(24) + y(24), x(25) + y(25), x(26) + y(26), x(27) + y(27), x(28) + y(28), x(29) + y(29),
      x(30) + y(30), x(31) + y(31), x(32) + y(32), x(33) + y(33), x(34) + y(34), x(35) + y(35), x(36) + y(36), x(37) + y(37), x(38) + y(38), x(39) + y(39),
      x(40) + y(40), x(41) + y(41), x(42) + y(42), x(43) + y(43), x(44) + y(44), x(45) + y(45), x(46) + y(46), x(47) + y(47), x(48) + y(48), x(49) + y(49),
      x(50) + y(50), x(51) + y(51), x(52) + y(52), x(53) + y(53), x(54) + y(54), x(55) + y(55), x(56) + y(56), x(57) + y(57), x(58) + y(58), x(59) + y(59),
      x(60) + y(60), x(61) + y(61), x(62) + y(62), x(63) + y(63), x(64) + y(64), x(65) + y(65), x(66) + y(66), x(67) + y(67), x(68) + y(68), x(69) + y(69),
      x(70) + y(70), x(71) + y(71), x(72) + y(72), x(73) + y(73), x(74) + y(74), x(75) + y(75), x(76) + y(76), x(77) + y(77), x(78) + y(78), x(79) + y(79),
      x(80) + y(80), x(81) + y(81), x(82) + y(82), x(83) + y(83), x(84) + y(84), x(85) + y(85), x(86) + y(86), x(87) + y(87), x(88) + y(88), x(89) + y(89)))

    // 3.6 Label Point
    val testing = main_data_agg.map(r => (r._1._6, Array(r._1._5, r._1._7, r._1._8) ++ r._2)).map(r => LabeledPoint(r._1, Vectors.dense(r._2))).cache()
    /* Target: 0.repeater,     
       Features: 1.repeattrips, 2.quantity, 3.offervalue, 4~86.(72 features)   
     */


    // 3.8 Logistic Regression
    // fixed hyperparameters
    
    // Compute raw scores on the test set.
    val lgModelL1 = LogisticRegressionModel.load(sc, model_path)
    val scoreAndLabels_lg = testing.map { point =>
      val score = lgModelL1.predict(point.features)
      (score, point.label)
    }

    scoreAndLabels_lg.foreach(println)
    
  }
}