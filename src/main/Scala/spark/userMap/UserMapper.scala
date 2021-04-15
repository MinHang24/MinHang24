package spark.userMap

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.Utils.GetID
import scala.collection.mutable.HashMap
import scala.collection.mutable

/**
 * @ObjectName UserMapper
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/14 8:46
 * @Version 1.0
 */
object UserMapper {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println(
        """
          |inputPath
          |outputPath
          |mapPath
          |缺少参数""".stripMargin)
      sys.exit()
    }
    val Array(inputPath,mapPath) = args
    val conf: SparkConf = new SparkConf().setAppName("MediaAnys").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile(inputPath)
    //  imei：3472934723472934    LC01->1,LNbanner->1,APP爱奇艺->1`CNxxx->1,D00010001->1,D00020001>1
    val rdd1: RDD[(String, (String, Int))] = rdd.map(line => {
      val strs: Array[String] = line.split(":")
      (GetID.get(strs), ("LC"+strs(33), 1))
    })
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd1.groupByKey()
    rdd2.mapValues(_.toMap.foldLeft("",0){
      case (m: ( String,  Int), (k: String, v: Int)) =>
        m + (k -> (m._2 + v))
    })





  }

}
