import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3


object streamingTest {
  def main(args: Array[String]): Unit = {
    test()
  }

  def test(): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words=lines.flatMap(_.split(" "))
    val pairs=words.map(word=>(word,1))
    val wordCounts=pairs.reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
