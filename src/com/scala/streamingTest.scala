import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream // not necessary since Spark 1.3


object streamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    test(lines)
    ssc.start()
    ssc.awaitTermination()
  }

  def test(lines: ReceiverInputDStream[String]): Unit = {
    //无状态
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
  }

  def testState(lines: ReceiverInputDStream[String]): Unit = {
    //跟时间区间有关系的
    //基于窗口的转化操作：窗口时长，滑动步长
    val words = lines.flatMap(_.split(" "))
    val pairsDStream = words.map(word => (word, 1))
    val accessLogsWindow = pairsDStream.window(Seconds(30), Seconds(10))
    val windowCounts = accessLogsWindow.count()
    val pairCountDStream = pairsDStream.reduceByKeyAndWindow(
      _ + _,
      _ - _,
      Seconds(30),
      Seconds(10)
    )
//    pairCountDStream.saveAsHadoopFiles[SequenceFileOutputFormat[Text, LongWritable]]("outputDis", "txt")
  }

  //响应代码计数
  def testUpdateState(lines: ReceiverInputDStream[String]): Unit = {
    val codes = lines.map(code => (code, 1L))
    //    val response=codes.updateStateByKey(updateRunningSum _)
  }

  def updateRunningSum(values: Seq[Long], state: Option[Long]) {
    Some(state.getOrElse(0L) + values.size);
  }

  //容错
  def createStreamingContext()={
    val sc=new SparkContext()
    val ssc=new StreamingContext(sc,Seconds(1))
    ssc.checkpoint("")
  }
  def testContext()={
//    val ssc=StreamingContext.getOrCreate("",createStreamingContext _)
  }

}
