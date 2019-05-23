import org.apache.spark.{SparkConf, SparkContext}

object HighLevelAPI {

  private val sc = Util.getSc()


  def main(args: Array[String]): Unit = {
//    counter()
    removeNormalVal()
  }

  //共享变量
  def counter(): Unit = {
    val path = Util.directory + "/count"
    val file = sc.textFile(path)

    val blankLines = sc.longAccumulator


    val callSigns = file.flatMap(line => {
      if (line == "") {
        blankLines.add(1)
      }
      line.split(" ")
    })

    callSigns.saveAsTextFile(Util.directory + "/count_res")

    println("Blank lines： " + blankLines.value)
  }

  //共享大对象
  def test(): Unit = {
    //    val signPrefixed=sc.broadcast()
    //    val countryContactCounts=contactCounts.map{
    //      case (sign,count)=>
    //        val country=lookupInArray(sign,signPrefixes.value)
    //        (country,count)
    //    }.reduceByKey((x,y)=>x+y)
    //    countryContactCounts.saveAsTextFile(outputDir + "/countries.txt");
    //可以使用spark.serializer用来优化序列化速度
  }

  //基于分区进行操作
  def processCallSign(): Unit = {
    //    val contactsContactLists=validSigns.distinct().mapPartitions{
    //      signs=>
    //        val mapper=createMapper()
    //        val client=new HttpClient()
    //        client.start()
    //        //创建http请求
    //        signs.map
    //          sign=>
    //            createExchangeForSign()
    //        }.map{
    //          case(sign,exchange)=>(sign,readExchangeCallLog(mapper,exchange))
    //        }.filter(x=>x._2!=null)
    //    }
  }

  //统计值，流操作

  def removeNormalVal(): Unit = {
    val distance = sc.textFile(Util.directory + "distance")
    val distanceDouble = distance.map(staring => staring.toDouble)
    val stats = distanceDouble.stats()
    val stddev = stats.stdev
    println(stddev)
    val mean = stats.mean
    val reasonableDistances = distanceDouble.filter(x =>{
      println(math.abs(x - mean))
      println(3 * stddev)
      math.abs(x - mean) < 3 * stddev
    })
    println(reasonableDistances.collect().toList)
  }

}
