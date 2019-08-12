package LR

object LRPractise {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("Logistic_Prediction")
        .master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val bank_Marketing_Data=spark.read
      .option("header",true)
      .option("inferSchema","true")
      .csv("/opt/train/bank_marketing_data.csv"

    val selected_Data=bank_Marketing_Data.select("age",
      "job","marital","housing","loan","duartion","previous","poutcome","empvarrate","y")
      .withColumn("age",bank_Marketing_Data("age").cast(DoubleType))
      .withColumn("ducation",bank_Marketing_Data("duration").cast(DoubleType))
      .withColumn("previous",bank_Marketing_Data("previous").cast(DoubleType))

    selected_Data.show(5)
    println("data count:"+selected_Data.count())

    val summary=selected_Data.describe();
    summary.show())

    val columnNames=selected_Data.columns
    val uniqueValues_Prefield=columnNames.map{
      field=>
        field+":"+selected_Data.select(field).distinct.count()
    }
    uniqueValues_Prefield.foreach(println)


  }
}
