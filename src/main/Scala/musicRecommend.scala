import org.apache.spark.mllib.recommendation._

object musicRecommend {

  def music(): Unit = {
    val sc = Util.getSc()
    val rawUserArtistData = sc.textFile("hdfs:///user/ds/user_artist_data.txt")

    rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    rawUserArtistData.map(_.split(' ')(0).toDouble).stats()

    val rawArtistData = sc.textFile("hdfs:///user/ds/artist_data.txt")
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t');
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toIndexedSeq, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

    val rawArtiastAlias = sc.textFile("hdfs:///user/ds/artist_alias.txt")
    val artistAlias = rawArtiastAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

    val bArtistAlias = sc.broadcast(artistAlias)

    val trainData = rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }.cache()

    val model = ALS.trainImplicit(trainData,10,5,0.01,1.0)
  }
}
