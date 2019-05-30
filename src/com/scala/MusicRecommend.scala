import org.apache.log4j.BasicConfigurator
import org.apache.spark.mllib.recommendation._

object MusicRecommend {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    music()
  }

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
    val artistById = rawArtistData.flatMap {
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
        val tokens= line.split("\t")
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

  }
}
