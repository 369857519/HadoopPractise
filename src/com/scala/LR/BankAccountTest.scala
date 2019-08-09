package musicRecommend

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class RecommendBase(private val spark: SparkSession) {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val bank_Marketing_Data=spark.read
    .option("header",true)
    .option("inferSchema","true")
    .csv("/opt/train/bank_marketing_data.csv"

  val select_Data=bank_Marketing_Data.select("age",
  "job","marital","housing","loan","duartion","previous","poutcome","empvarrate","y")
  .withColumn("age",bank_Marketing_Data("age").cast(DoubleType))
  .withColumn("ducation",bank_Marketing_Data("duration").cast(DoubleType))
  .withColumn("previous",bank_Marketing_Data("previous").cast(DoubleType))

  selected_Data.show(5)
  println("data count:"+selected_Data.count())


}
