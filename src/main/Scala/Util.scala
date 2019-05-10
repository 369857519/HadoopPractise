import org.apache.spark.{SparkConf, SparkContext}

object Util {
  def getSc(): SparkContext ={
    val conf=new SparkConf().setAppName("mySpark")
    conf.setMaster("local")
    val sc=new SparkContext(conf)
    return sc
  }
  val directory="file:///Users/qilianshan/Documents/videoSpace/hadoopPractise/src/main/resources/"

}
