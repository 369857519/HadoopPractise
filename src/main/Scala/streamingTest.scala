import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamingTest {
  def main(args: Array[String]): Unit = {
    test()
  }

  def test(): Unit ={
    val outPutPath=Util.directory+"/streamingOutput"
    val conf=new SparkConf().setAppName("mySpark")
    conf.setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(1))
    val lines = ssc.socketTextStream("localhost",7777)
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()
    ssc.textFileStream(outPutPath)
    ssc.start()
    ssc.awaitTermination()
  }
}
