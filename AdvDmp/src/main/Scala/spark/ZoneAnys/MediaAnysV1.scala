package spark.ZoneAnys

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import spark.Utils.ZoneTool
import spark.bean.logBean

/**
 * @ObjectName MediaAnysV1
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 23:03
 * @Version 1.0
 */
object MediaAnysV1 {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
          |appmapping
          |outputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputPath,mapperPath,outputPath) = args
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val session: SparkSession = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = session.sparkContext

    val mapper: RDD[String] = sc.textFile(mapperPath)
    val map: Map[String, String] = mapper.map(line => {
      val arr: Array[String] = line.split("[:]", -1)
      (arr(0), arr(1))
    }).collect().toMap
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(map)

    val rdd: RDD[String] = sc.textFile(inputPath)
    val log: RDD[logBean] = rdd.map(_.split(",", -1)).filter(_.length >= 85).map(logBean(_)).filter(t => !t.appid.isEmpty || !t.appname.isEmpty)
    val res: RDD[(String, List[Double])] = log.map(log => {
      val qqs: List[Double] = ZoneTool.qqsRtp(log.requestmode, log.processnode)
      var appname: String = log.appname
      if (appname == "" || appname.isEmpty) {
        appname = broadcast.value.getOrElse(log.appid, "不明确")
      }
      (appname, qqs)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
    res.saveAsTextFile(outputPath)
    sc.stop()
  }

}
