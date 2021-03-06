package musicRecommend

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

class RunRecommender(private val spark: SparkSession) {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  private val recommendBase = new RecommendBase(spark)
  
  def preparation(rawUserArtistData: Dataset[String], rawArtistData: Dataset[String], rawArtistAlias: Dataset[String]): Unit = {
    rawUserArtistData.take(5).foreach(println)
    val userArtistDF = rawUserArtistData.map {
      line =>
        val Array(user, artist, _*) = line.split(' ')
        (user.toInt, artist.toInt)
    }.toDF("user", "artist")
    userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()
    val artistByID = buildArtistByID(rawArtistData)
    val artistAlias = recommendBase.buildArtistAlias(rawArtistAlias)
    val (badID, goodID) = artistAlias.head
    artistByID.filter($"id" isin(badID, goodID)).show()
  }

  def buildArtistByID(rawArtistData: Dataset[String]): DataFrame = {
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name")
  }


  def model(rawUserArtistData: Dataset[String], rawArtistData: Dataset[String], rawArtistAlias: Dataset[String]): Unit = {
    val bArtistAlias = spark.sparkContext.broadcast(recommendBase.buildArtistAlias(rawArtistAlias))

    val trainData = recommendBase.buildCounts(rawUserArtistData, bArtistAlias).cache()
    //implicitPref 隐式参数
    //regParam 正则化参数()
    //predictionCol
    val model = new ALS().setSeed(Random.nextLong()).setImplicitPrefs(true)
      .setRank(10).setRegParam(0.01).setAlpha(1.0).setUserCol("user").setItemCol("artist")
      .setRatingCol("count").setPredictionCol("prediction").fit(trainData)

    trainData.unpersist()
    model.userFactors.select("features").show(truncate = false)

    val userID = 2093760

    val existingArsitsIDs = trainData.filter($"user" === userID).select("artist").as[Int].collect()

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter($"id" isin (existingArsitsIDs: _*)).show()

    val topRecommendations = makeRecommendations(model, userID, 5)
    topRecommendations.show()

    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()

    artistByID.filter($"id" isin (recommendedArtistIDs: _*)).show()

    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }


  def makeRecommendations(model: ALSModel, userID: Int, outputCount: Int): DataFrame = {
    val toRecommend = model.itemFactors.select($"id".as("artist")).withColumn("user", lit(userID))
    model.transform(toRecommend).select("artist", "prediction").orderBy($"prediction".desc).limit(outputCount)
  }

}