package utilClasses

/**
 * @author ivanliu
 */
class utility {
  
  val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  
  def diff_days(s1: String, s2: String, TimeUnit timeUnit) = {
    val date1 = date_format.parse(s1)
    val date2 = date_format.parse(s2)
    val delta: Long = date2.getTime() - date1.getTime()
    return timeUnit.convert(delta,TimeUnit.MILLISECONDS)
  }
  
  def generate_feature(loc_train: String, loc_test: String, loc_transactions: String, loc_out_train: String, loc_out_test: String) = {
    
  }
}
