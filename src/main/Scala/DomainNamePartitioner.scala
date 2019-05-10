import java.net.URL

import org.apache.spark.Partitioner

class DomainNamePartitioner(numParts: Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain=new URL(key.toString).getHost()
    val code= domain.hashCode() % numPartitions
    if(code<0){
      code+numPartitions
    }else{
      code
    }
  }

//  override def equals(obj: scala.Any): Boolean = other match{
//    case dnp:DomainNamePartitioner=>
//      dnp.numPartitions==numPartitions
//    case _=>
//      false
//  }
}
