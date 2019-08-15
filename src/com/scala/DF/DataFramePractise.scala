package DF

import org.apache.spark.sql.SparkSession

case class Incedents(incidentnum: String, category: String, description: String, dayofweek: String, date: String, time: String, pddistrict: String, resolution: String, address: String, x: String, y: String, location: String, pdid: String)

object DataFramePractise {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Logistic_Prediction")
      .master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val sfpdRDD = spark.read.text("D:\\dataSpace\\SFPD.csv")
    val sfpdDF = sfpdRDD.map {
      inc =>
        inc.toString().split(",")
    }.map {
      inc =>
        Incedents(inc(0), inc(1), inc(2), inc(3), inc(4), inc(5), inc(6).toString, inc(7).toString, inc(8).toString, inc(9).toString, inc(10).toString, inc(11).toString, inc(12).toString)
    }.toDF()
    sfpdDF.show(10)
    //    sfpdCase.describe().show()
    //    sfpdDF.printSchema()
    //取top3
    val incByAdd = sfpdDF.groupBy("address")
    val numAdd = incByAdd.count()
    val numAddDesc = numAdd.sort($"count".desc)
    sfpdDF.createOrReplaceTempView("sfpd");
    numAddDesc.show(3)
    val sqlContext = sfpdDF.sqlContext
    val top3Address = sfpdDF.sqlContext.sql("SELECT address,count(incidentnum) as inccount" +
      " FROM sfpd GROUP BY address ORDER BY inccount DESC LIMIT 3").show

    //自定义函数
    val getYearString = (s: String) => {
      val lastString = s.substring(s.lastIndexOf('/) + 1)
    }

    //    sfpdDF.sqlContext.udf.register("getYearString",getYearString)
    //    val year=sfpdDF.groupBy(getYearString(sfpdDF("Date"))).count.show
    def getString(s: String) = {
      val stringAfter = s.substring(s.lastIndexOf('/') + 1); stringAfter
    }

    sqlContext.udf.register("getString", getString _)
    val numOfIncByYear = sfpdDF.sqlContext.sql("SELECT getString(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY getString(date) ORDER BY countbyyear DESC LIMIT 5");
    numOfIncByYear.show()
  }
}
