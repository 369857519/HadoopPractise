import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SparkSession}

object MLTest {
    //spark只包含能够良好并行执行的算法，包括一些新的算法：随机森林，K-means聚类，交替最小二乘法
    //线性回归，逻辑回归，支持向量机，朴素贝叶斯，决策树与决策森林
    //Kmeans，交替最小二乘
    //降维

  case class LabeledDocument(id:Long,text:String,label:Double)

  val documents = Util.getSc().parallelize(List(123))//

  var sqlContext = new sql.SparkSession.Builder()

  val tokenizer=new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")
  val tf=new HashingTF()
    .setNumFeatures(10000)
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("features")
  val lr=new LogisticRegression()
  val pipline=new Pipeline().setStages(Array(tokenizer,tf,lr))

//  val model=pipline.fit(documents)
  val paramMaps=new ParamGridBuilder()
    .addGrid(tf.numFeatures,Array(10000,20000))
    .addGrid(lr.maxIter,Array(100,200))
    .build()
  val eval = new BinaryClassificationEvaluator()
  val cv=new CrossValidator()
    .setEstimator(lr)
    .setEstimatorParamMaps(paramMaps)
    .setEvaluator(eval)
//  val bestModel=cv.fit(documents)
}

