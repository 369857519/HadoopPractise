import musicRecommend.{RunEvaluate, RunRecommender}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//hdfs://localhost/user/ds/artist_data.txt"
object MusicRecommend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    System.setProperty("hadoop.home.dir", "/Users/qilianshan/Documents/env/hadoop-2.9.2")
    conf.set("spark.sql.crossJoin.enabled", "true")
    val spark = SparkSession.builder().master("local[2]").appName("mySpark").config(conf).getOrCreate()
    //    spark.sparkContext.setCheckpointDir("hdfs://localhost/tmp/")

    val base = "hdfs://localhost/user/ds/"
    val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

    //    val runRecommender = new RunRecommender(spark)
    //    runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
    //    runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
    val runEvaluate = new RunEvaluate(spark)
    runEvaluate.evaluate(rawUserArtistData, rawArtistAlias)
  }
}


