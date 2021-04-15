package spark.ZoneAnys

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import spark.Utils.ZoneTool
import spark.bean.logBean

import scala.collection.mutable.ListBuffer

/**
 * @ObjectName MediaAnysV2
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 23:15
 * @Version 1.0
 */
object MediaAnysV2 {


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
          |outputPath
        """.stripMargin)
      sys.exit()
    }

    // 接收参数
    val Array(inputPath, outputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputPath)
    val res: RDD[(String, List[Double])] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(logBean(_)).filter(log => !log.appid.isEmpty)
      .mapPartitions(item => {
        val tuple = new ListBuffer[(String, List[Double])]
        val resource: Jedis = JedisUtil.resource
        item.foreach(log => {
          var appname: String = log.appname
          if (appname.isEmpty) {
            appname = resource.get(log.appid)
          }
          val qqs: List[Double] = ZoneTool.qqsRtp(log.requestmode, log.processnode)
          tuple += ((appname, qqs))
        })
        tuple.iterator
      })
    res.reduceByKey((list1,list2) => {
      list1.zip(list2).map(t => t._1+t._2)
    }).map(t => t._1+"\t"+t._2.mkString(",")).saveAsTextFile(outputPath)
  }
}
