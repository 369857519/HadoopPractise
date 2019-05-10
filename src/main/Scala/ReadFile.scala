import org.apache.spark.{SparkConf, SparkContext}

object ReadFile {
  def main(args: Array[String]) {
    readFile()
  }
  case class Person(name:String,lovesPandas:Boolean)
  def readFile(): Unit ={
    val conf=new SparkConf().setAppName("mySpark")
    val directory="file:///Users/qilianshan/Documents/videoSpace/hadoopPractise/src/main/resources/"
    var path=directory+"/count"
    conf.setMaster("local")
    val sc=new SparkContext(conf)
    //part-*.txt
    val input=sc.wholeTextFiles(path)
    val result=input.mapValues{y=>
      val nums=y.split(" ").map(x=>x.toDouble)
      nums.sum/nums.size.toDouble
    }
    println(result.collect().mkString(","))
    result.saveAsTextFile(path+"_res")


    //读取json
//    val path2=directory+"/person.json"
//    val input2=sc.wholeTextFiles(path2)
//    val result2=input2.flatMap(record=>{
//      try{
//        Some(mapper.readValue(record,classOf[Person]))
//      } catch{
//        case e:Exception=>None
//      }
//    })

  }
}
