import org.apache.spark.SparkContext

class PageRank {
  var sc=new SparkContext();
  var ranks=sc.parallelize(List((1,1.0),(2,1.0),(3,1.0)))
  val links=sc.parallelize(List((1,List(1,2,3)),(2,List(3)),(3,List(2))));

  for(i <- 0 until 10){
    val contributions=links.join(ranks).flatMap{
      case (pageId,(links,rank))=>
        links.map(dest=>(dest,rank/links.size))
    }
    ranks=contributions.reduceByKey((x,y)=>x+y).mapValues(v=>0.15+0.85*v)
  }

  ranks.saveAsTextFile("ranks");
}
