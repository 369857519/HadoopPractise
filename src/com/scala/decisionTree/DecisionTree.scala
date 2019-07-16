package decisionTree

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.sql.SparkSession

object DecisionTree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    System.setProperty("hadoop.home.dir", "/Users/qilianshan/Documents/env/hadoop-2.9.2")
    conf.set("spark.sql.crossJoin.enabled", "true")
    val spark = SparkSession.builder().master("local[2]").appName("mySpark").config(conf).getOrCreate()
    import spark.implicits._

    val dataWithoutHeader = spark.read.option("inferSchema", true).option("header", false).csv("hdfs://localhost/user/ds/covtype.data")
    val colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++
      (0 until 4).map(i => s"Wilderness_Area_$i") ++ (
      (0 until 40).map(i => s"Soil_Type_$i")
      ) ++ Seq("Cover_Type")

    val data = dataWithoutHeader.toDF(colNames: _*).withColumn("Cover_Type", $"Cover_Type".cast("double"))
    data.show()
    data.head

    val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    testData.cache()

    val runRDF = new RunRDF(spark)
    runRDF.simpleDecisionTree(trainData, testData)
  }
}
