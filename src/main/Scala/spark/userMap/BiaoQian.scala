package spark.userMap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import spark.Utils.GongJu
import spark.bean.logBean

object BiaoQian {

  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 3) {
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
    val Array(inputPath,mappPath,outputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //获取文件   logBean文件
    val rdd: RDD[String] = sc.textFile(inputPath)
    //对文件进行清洗:
    val log: RDD[logBean] = rdd.map(_.split(",", -1))
      .filter((_: Array[String]).length >= 85)
      .map(logBean(_))
    val rddd: RDD[(String, ((String, String), Int))] = log.map(logBean => {
      val imei: String = logBean.imei
      val mac: String = logBean.mac
      val idfa: String = logBean.idfa
      val openudid: String = logBean.openudid
      val androidid: String = logBean.androidid
      val id: String = GongJu.getKey(imei, mac, idfa, openudid, androidid)
      val adspacetype: Int = logBean.adspacetype
      val adspacetypename: String = logBean.adspacetypename
      val adspace: String = GongJu.adspace(adspacetype)
      val adspacename: String = GongJu.adspacename(adspacetypename)
      (id, ((adspace, adspacename), 1))
    })
    val ddd: RDD[(String, ((String, String), Int))] = rddd.reduceByKey((x, y) => {
      (x._1, x._2 + x._2)
    })
    //输出结果如下:(6c892d8bca7fd1e8,((LC09->,LN视频暂停悬浮->),4))

    val value: RDD[String] = ddd.map(x => {
      x._1 + "," + x._2._1._1+x._2._2+","+  x._2._1._2+x._2._2
    })
    //输出结果如下: 6377acc5ad6c0087,LC09->2,LN视频暂停悬浮->2
    value.foreach(println(_))

  }

}
