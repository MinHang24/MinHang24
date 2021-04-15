package spark.bean

import org.apache.spark.Partitioner

import scala.collection.mutable

/**
 * @ObjectName ProvincePartition
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/12 16:57
 * @Version 1.0
 */
class ProvincePartition(data:Set[String]) extends Partitioner{

  override def numPartitions: Int = data.toList.length

  override def getPartition(key: Any): Int = {
    data.toList.indexOf(key)
  }
}
