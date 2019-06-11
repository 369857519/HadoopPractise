package musicRecommend

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

class RunEvaluate(private val spark: SparkSession) {

  import org.apache.spark.sql.functions._
  import spark.implicits._


  private val recommendBase = new RecommendBase(spark)

  def evaluate(
                rawUserArtistData: Dataset[String],
                rawArtistAlias: Dataset[String]): Unit = {

    val bArtistAlias = spark.sparkContext.broadcast(recommendBase.buildArtistAlias(rawArtistAlias))

    val allData = recommendBase.buildCounts(rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allArtistIDs = allData.select("artist").as[Int].distinct().collect()
    val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

    val mostListenedAUC = recommendBase.areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
    println(mostListenedAUC)

    val evaluations =
      for (rank <- Seq(5, 30);
           regParam <- Seq(1.0, 0.0001);
           alpha <- Seq(1.0, 40.0))
        yield {
          val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(rank).setRegParam(regParam).
            setAlpha(alpha).setMaxIter(20).
            setUserCol("user").setItemCol("artist").
            setRatingCol("count").setPredictionCol("prediction").
            fit(trainData)

          val auc = recommendBase.areaUnderCurve(cvData, bAllArtistIDs, model.transform)

          model.userFactors.unpersist()
          model.itemFactors.unpersist()

          (auc, (rank, regParam, alpha))
        }

    evaluations.sorted.reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
    val listenCounts = train.groupBy("artist").
      agg(sum("count").as("prediction")).
      select("artist", "prediction")
    allData.
      join(listenCounts, Seq("artist"), "left_outer").
      select("user", "artist", "prediction")
  }
}
