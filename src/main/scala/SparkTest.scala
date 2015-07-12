/**
 * Created by ivanliu on 12/07/15.
 */

/**
 * Created by ivanliu on 12/07/15.
 */

import org.apache.spark._
import org.apache.spark.streaming._

object SparkTest  {


  val checkpoint_dir = "./checkpoint"

  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("Streaming Example").setMaster("local[2]")

    val ssc = StreamingContext.getOrCreate(checkpoint_dir, ()=>{CreateStreamingContext(conf, 10, 40)})

    ssc.start
    ssc.awaitTermination
  }

  def CreateStreamingContext(conf:SparkConf, interval:Int, windowLength:Int=0):StreamingContext = {

    val ssc = new StreamingContext(conf, Seconds(interval))
    ssc.checkpoint(checkpoint_dir)
    val lines = ssc.socketTextStream("localhost",9999)

    lines.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKeyAndWindow((a:Int, b:Int)=>(a+b), Seconds(windowLength), Seconds(interval)).print

    ssc
  }

}