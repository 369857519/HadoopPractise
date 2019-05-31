import org.apache.log4j.BasicConfigurator
import org.apache.spark.mllib.recommendation._

object MusicRecommend {


  def music(): Unit = {
    val sc = Util.getSc()
    //用户对于艺术家的访问次数 用户 艺术家 分数
    val rawUserArtistData = sc.textFile("hdfs://localhost/user/ds/user_artist_data.txt")

    //    val userIds = rawUserArtistData.map(_.split(' ')(0).toDouble)
    //    println(userIds.stats().toString())
    //    val artistIds = rawUserArtistData.map(_.split(' ')(1).toDouble)
    //    println(artistIds.stats().toString())

    val rawArtistData = sc.textFile("hdfs://localhost/user/ds/artist_data.txt")

    //这里的flatmap是把结果中的someflat掉。
    val artistByID = rawArtistData.flatMap {
      line =>
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) {
          None
        } else {
          try {
            Some((id.toInt, name.trim))
          } catch {
            case e: NumberFormatException => None
          }
        }
    }

    val rawArtistAlias = sc.textFile("hdfs://localhost/user/ds/artist_alias.txt")
    val artistAlias = rawArtistAlias.flatMap {
      line =>
        val tokens = line.split("\t")
        if (tokens(0).isEmpty) {
          None
        } else {
          try {
            Some((tokens(0).toInt, tokens(1).toInt))
          } catch {
            case e: NumberFormatException => None
          }
        }
    }.collectAsMap()
    //    artistAlias.take(10).foreach(println)

    //如果不做广播变量， artistAlias会随着任务一起被复制，每个任务会有一个闭包存储，JVM中可能会运行多个任务，耗费内存
    //做个广播变量以后，每个executor内只发送一个副本，节省巨大的网络流量和内存
    val bArtistAlias = sc.broadcast(artistAlias);

    val trainData = rawUserArtistData.map {
      line =>
        val Array(userID, artistID, score) = line.split(" ").map(_.toInt)
        val realArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
        Rating(userID, realArtistID, score.toDouble)
    }.cache()
    //als是迭代运算的，所以训练数据最好做一个cache，否则需要反复运算

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    //als模型训练结束后会有用户feature和productfeature两个属性
    //    model.userFeatures.mapValues(_.mkString(",")).take(10).map(println)
    //查看数据
    val recommendations = model.recommendProducts(2093760, 5);
    recommendations.foreach(println)

    val recommendProductIDs=recommendations.map(_.product).toSet

    artistByID.filter{
      case(id,_)=>recommendProductIDs.contains(id)
    }.values.collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
        music()
//    usersRealPreference()
  }

  def usersRealPreference(): Unit = {
    val sc = Util.getSc()
    val rawUserArtistData = sc.textFile("hdfs://localhost/user/ds/user_artist_data.txt")
    val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).filter {
      case Array(user, _, _) => user.toInt == 2093760
    }

    val existingProducts = rawArtistsForUser.map {
      case Array(_, artist, _) => artist.toInt
    }.collect().toSet

    val rawArtistData = sc.textFile("hdfs://localhost/user/ds/artist_data.txt")

    //这里的flatmap是把结果中的someflat掉。
    val artistByID = rawArtistData.flatMap {
      line =>
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) {
          None
        } else {
          try {
            Some((id.toInt, name.trim))
          } catch {
            case e: NumberFormatException => None
          }
        }
    }

    artistByID.filter {
      case (id, name) => existingProducts.contains(id)
    }.values.collect().foreach(println)
  }
}
