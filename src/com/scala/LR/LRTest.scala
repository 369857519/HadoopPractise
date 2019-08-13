package LR

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object LRTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Logistic_Prediction")
      .master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val bank_Marketing_Data = spark.read
      .option("header", true)
      .option("inferSchema", "true")
      .csv("D:\\dataSpace\\bank_marketing_data.txt")

//    bank_Marketing_Data.show(10)

    val selected_data = bank_Marketing_Data.select("age",
      "job", "marital", "housing", "loan", "duration", "previous", "poutcome", "empvarrate", "y")
      .withColumn("age", bank_Marketing_Data("age").cast(DoubleType))
      .withColumn("duration", bank_Marketing_Data("duration").cast(DoubleType))
      .withColumn("previous", bank_Marketing_Data("previous").cast(DoubleType))

    selected_data.describe().show()
    val columnNames=selected_data.columns
    val uniqueValues_PreField=columnNames.map{
      field=>
        field+":"+selected_data.select(field).distinct().count()
    }
    uniqueValues_PreField.map(println)


    val indexer = new StringIndexer().setInputCol("job").setOutputCol("jobIndex")
    val indexed = indexer.fit(selected_data).transform(selected_data)
    indexed.show
    val encoder = new OneHotEncoder().setDropLast(false).setInputCol("jobIndex").setOutputCol("jobVec")
    val encoded = encoder.transform(indexed)

    val maritalIndexer = new StringIndexer().setInputCol("marital").setOutputCol("maritalIndex")
    //注意：此处所使用的数据是job列应用OneHotEncoder算法后产生的数据encoded
    //这里不能使用原始数据selected_Data，因为原始数据中没有jobVec列。
    val maritalIndexed = maritalIndexer.fit(encoded).transform(encoded)
    val maritalEncoder = new OneHotEncoder().setDropLast(false).setInputCol("maritalIndex").setOutputCol("maritalVec")
    val maritalEncoded = maritalEncoder.transform(maritalIndexed)

    val defaultIndexer = new StringIndexer().setInputCol("default").setOutputCol("defaultIndex")
    //注意：此处所使用的数据是对marital列应用OneHotEncoder算法后产生的数据maritalEncoded
    val defaultIndexed = defaultIndexer.fit(maritalEncoded).transform(maritalEncoded)
    val defaultEncoder = new OneHotEncoder().setDropLast(false).setInputCol("defaultIndex").setOutputCol("defaultVec")
    val defaultEncoded = defaultEncoder.transform(defaultIndexed)

    val housingIndexer = new StringIndexer().setInputCol("housing").setOutputCol("housingIndex")
    //注意：此处所使用的数据是对default列应用OneHotEncoder算法后产生的数据defaultEncoded
    val housingIndexed = housingIndexer.fit(defaultEncoded).transform(defaultEncoded)
    val housingEncoder = new OneHotEncoder().setDropLast(false).setInputCol("housingIndex").setOutputCol("housingVec")
    val housingEncoded = housingEncoder.transform(housingIndexed)

    val poutcomeIndexer = new StringIndexer().setInputCol("poutcome").setOutputCol("poutcomeIndex")
    //注意：此处所使用的数据是对housing列应用OneHotEncoder算法后产生的数据housingEncoded
    val poutcomeIndexed = poutcomeIndexer.fit(housingEncoded).transform(housingEncoded)
    val poutcomeEncoder = new OneHotEncoder().setDropLast(false).setInputCol("poutcomeIndex").setOutputCol("poutcomeVec")
    val poutcomeEncoded = poutcomeEncoder.transform(poutcomeIndexed)

    val loanIndexer = new StringIndexer().setInputCol("loan").setOutputCol("loanIndex")
    //注意：此处所使用的数据是对poutcome列应用OneHotEncoder算法后产生的数据poutcomeEncoded
    val loanIndexed = loanIndexer.fit(poutcomeEncoded).transform(poutcomeEncoded)
    val loanEncoder = new OneHotEncoder().setDropLast(false).setInputCol("loanIndex").setOutputCol("loanVec")
    val loanEncoded = loanEncoder.transform(loanIndexed)
    loanEncoded.show()
    loanEncoded.printSchema()

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("jobVec", "maritalVec", "defaultVec", "housingVec", "poutcomeVec", "loanVec", "age", "duration", "previous", "empvarrate"))
      .setOutputCol("features")
    val indexerY = new StringIndexer().setInputCol("y").setOutputCol("label")
    val transformers = Array(indexer,
      encoder,
      maritalIndexer,
      maritalEncoder,
      defaultIndexer,
      defaultEncoder,
      housingIndexer,
      housingEncoder,
      poutcomeIndexer,
      poutcomeEncoder,
      loanIndexer,
      loanEncoder,
      vectorAssembler,
      indexerY)
    val splits = selected_data.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()
    val lr = new LogisticRegression()
    val model = new Pipeline().setStages(transformers :+ lr).fit(training)
    val result = model.transform(test)
    result.select("label", "prediction", "rawPrediction", "probability")
      .show(10, false)
    val evaluator = new BinaryClassificationEvaluator()
    var aucTraining = evaluator.evaluate(result)
    println("aucTraining = " + aucTraining)
  }
}
