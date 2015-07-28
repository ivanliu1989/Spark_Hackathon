package utilClasses

/**
 * @author ivanliu
 */
class utility {
  
  val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date_unit = 1.15741e-8
  
  def diff_days(s1: String, s2: String) = {
    val date1 = date_format.parse(s1)
    val date2 = date_format.parse(s2)
    val delta: Long = date2.getTime() - date1.getTime()
    Math.abs((delta * date_unit).toInt)
  }
  
  def generate_feature(loc_train: String, loc_test: String, loc_transactions: String, loc_out_train: String, loc_out_test: String) = {
    
  }
}
