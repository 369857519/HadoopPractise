import org.apache.spark.{SparkConf, SparkContext}

object Util {
  def getSc(): SparkContext ={
    val conf=new SparkConf().setAppName("mySpark")
    conf.setMaster("local[2]")
//    conf.setMaster("spark://qilianshans-iMac.local:7077")
    //spark.master  spark://qilianshans-iMac.local:7077
    System.setProperty("hadoop.home.dir","/Users/qilianshan/Documents/env/hadoop-2.9.2");
    val sc=new SparkContext(conf)
    return sc
  }
  val directory="file:///Users/qilianshan/Documents/videoSpace/hadoopPractise/src/main/resources/"

}
