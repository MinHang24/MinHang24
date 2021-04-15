package spark.logAnlyse

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import spark.bean.Mapper

/**
 * @ObjectName ZoneAnysSql
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 18:38
 * @Version 1.0
 */
object ZoneAnysSql {

  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println(
        """
          |缺少参数
          |inputPath
          |outputPath
          |mapperPath""".stripMargin)
      sys.exit()
    }
    val Array(inputPath,outputPath,mapperPath) = args
    val session: SparkSession = SparkSession.builder().appName("ZoneAnys").master("local[*]").getOrCreate()
    val sc: SparkContext = session.sparkContext


    val df: DataFrame = session.read.parquet(inputPath)
    df.createTempView("logData")


    val rdd: RDD[Array[String]] = sc.textFile(mapperPath).map(x => {
      val strs: Array[String] = x.split("[:]")
      strs
    })
    val rdd1: RDD[Mapper] = rdd.map(Mapper(_))
    val df1: DataFrame = session.createDataFrame(rdd1)
    df1.createTempView("mapperData")
    val sql =
      """
        |select appid,newname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end )as ysqq,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end )as yxqq,
        |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as jjx,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
        |from
        |(select l.*,
        |(case when l.appname == '' or l.appname is null or l.appname = '其他' then m.appname else l.appname end) as newname
        |from logData l
        |left outer join
        |mapperData m
        |on l.appid = m.appid) a
        |group by appid,newname""".stripMargin

    val df2: DataFrame = session.sql(sql)
    val df3: Dataset[Row] = df2.repartition(1)
    df3.write.csv(outputPath)
    session.stop()
    sc.stop()

  }

}
