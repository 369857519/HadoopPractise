import org.apache.log4j.BasicConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.evaluation.AreaUnderCurve
import org.apache.spark.rdd._

object MusicRecommend {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    music()
  }

  def areaUnderCurve(positiveData: RDD[Rating], bAllItemIDs: Broadcast[Array[Int]], predictFunction: RDD[(Int, Int)] => RDD[Rating]): Unit ={

  }
  def music(): Unit = {
    val sc = Util.getSc()
    //用户对于艺术家的访问次数 用户 艺术家 分数
    val rawUserArtistData = sc.textFile("hdfs://localhost/user/ds/user_artist_data.txt")

    //用户到艺术家的映射 12321312 321321 23213
    val rawArtistData = sc.textFile("hdfs://localhost/user/ds/artist_data.txt")
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t');
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

    //艺术家->艺术家 3123 23132
    val rawArtiastAlias = sc.textFile("hdfs://localhost/user/ds/artist_alias.txt")

    //艺术家->艺术家
    val artistAlias = rawArtiastAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()


    //广播变量
    val bArtistAlias = sc.broadcast(artistAlias)

    //用户 艺术家 分数
    val allData = rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }.cache()

    //训练
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0, 1))
    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs);

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    val auc = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))

    //同rawUserArtist 231232 2312 231，但是第一条指定了用户
    val rawArtistForUser = rawUserArtistData.map(_.split(' ')).filter {
      case Array(user, _, _) => user.toInt == 2093760
    }
    //获取上一行用户喜欢艺术家的ID
    val existingProducts = rawArtistForUser.map {
      case Array(_, artist, _) => artist.toInt
    }.collect().toSet
    //打印这个用户喜欢的所有艺术家
    val peopleLike = artistByID.filter {
      case (id, name) => existingProducts.contains(id)
    }.values.collect().foreach(println)

    val recommendations = model.recommendProducts(2093760, 5);
    recommendations.foreach(println);

    val recommendedProductIDs = recommendations.map(_.product).toSet
    //打印所有的为这位用户推荐的ID
    val peopleRecommend = artistByID.filter {
      case (id, name) => recommendedProductIDs.contains(id)
    }.values.collect().foreach(println)

  }

  def predictMostListened(sc: SparkContext, train: RDD[Rating])(allData: RDD[(Int, Int)]) = {
    val bListenCount = sc.broadcast(
      train.map(r => (r.product, r.rating))
        .reduceByKey(_ + _).collectAsMap()
    )
    allData.map {
      case (user, product) =>
        Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

}
