package spark.ZoneAnys

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import spark.Utils.ZoneTool
import spark.logAnlyse.ZoneAnys

import java.io

/**
 * @ObjectName MediaAnys
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 14:34
 * @Version 1.0
 */
object MediaAnys {

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
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = session.sparkContext
    val df: DataFrame = session.read.parquet(inputPath)
    val mapper: RDD[(String, String)] = sc.textFile(mapPath).map(line => {
      val strs: Array[String] = line.split(":")
      (strs(0), strs(1))
    })
    val map: Map[String, String] = mapper.collect().toMap
    val broad: Broadcast[Map[String, String]] = sc.broadcast(map)
    import session.implicits._
    val ds: Dataset[(String, List[Double])] = df.map(row => {
      val appid: String = row.getAs[String]("appid")
      var appname: String = row.getAs[String]("appname")
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      val ggzj: List[Double] = ZoneTool.ggzjRtp(requestmode, iseffective)
      val ggc: List[Double] = ZoneTool.ggcbRtp(iseffective, isbilling, iswin, winprice, adpayment)
      val qqs: List[Double] = ZoneTool.qqsRtp(requestmode, processnode)
      val mjj: List[Double] = ZoneTool.mjjRtp(requestmode, iseffective, isbilling)
      val jj: List[Double] = ZoneTool.jjRtp(iseffective, isbilling, isbid, iswin, adorderid)

      if (appname == null || appname == "" || appname.isEmpty) {
        appname = broad.value.getOrElse(appid,"不明确")
      }
      (appname, qqs ++ jj ++ ggzj ++ mjj ++ ggc)
    })
    ds.rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).foreach(println(_))
  }
}
