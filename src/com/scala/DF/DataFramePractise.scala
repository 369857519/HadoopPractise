package DF

import org.apache.spark.sql.SparkSession

case class Incedents(incidentnum:String, category:String, description:String, dayofweek:String, date:String, time:String, pddistrict:String, resolution:String, address:String, x:String, y:String, location:String, pdid:String)

object DataFramePractise {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Logistic_Prediction")
      .master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val sfpdRDD=spark.read.text("D:\\dataSpace\\SFPD.csv")
    val sfpdCase=sfpdRDD.map{
      inc=>
        inc.toString().split(",")
    }.map{
      inc=>
        Incedents(inc(0), inc(1), inc(2), inc(3), inc(4), inc(5), inc(6).toString, inc(7).toString, inc(8).toString, inc(9).toString, inc(10).toString, inc(11).toString, inc(12).toString)
    }.toDF()
    sfpdCase.show(10)
    sfpdCase.describe().show()
  }
}
