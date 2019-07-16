package decisionTree

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import scala.util.Random

class RunRDF(private val spark: SparkSession) {

  import spark.implicits._

  def simpleDecisionTree(trainData: DataFrame, testData: DataFrame): Unit = {
    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")

    val assembledTrainData = assembler.transform(trainData)
    assembledTrainData.select("featureVector").show(truncate = false)

    val classifier = new DecisionTreeClassifier()
      .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("featureVector")
      .setPredictionCol("prediction")
    val model = classifier.fit(assembledTrainData)
    println(model.toDebugString)

    model.featureImportances.toArray.zip(inputCols).sorted.reverse.foreach(println)

    val predictions = model.transform(assembledTrainData)
    predictions.select("Cover_type","prediction","probability").show(truncate = false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Cover_Type")
      .setPredictionCol("prediction")

    evaluator.setMetricName("accuracy").evaluate(predictions)
    evaluator.setMetricName("f1").evaluate(predictions)

  }
}
