package utilClasses
import java.io.{File,FileInputStream,FileOutputStream}

/**
 * @author ivanliu
 */
object utility {

  // 1. Calculate Date Difference
  def diff_days(s1: String, s2: String) = {
    val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date_unit = 1.15741e-8
    val date1 = date_format.parse(s1)
    val date2 = date_format.parse(s2)
    val delta: Long = date1.getTime() - date2.getTime()
    (delta * date_unit).toInt
  }
  
  // 2. Move predicted file
  def moveFile(srcPath: String, destPath: String)={
    val src = new File(srcPath)
    val dest = new File(destPath)
    new FileOutputStream(dest) getChannel() transferFrom(
    new FileInputStream(src) getChannel, 0, Long.MaxValue )
  }
  
}
