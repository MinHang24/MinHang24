package spark.logAnlyse

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ObjectName ZoneAnys
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 9:35
 * @Version 1.0
 */
object ZoneAnys {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println(
        """
          |缺少参数
          |inputPath
          |outputPath""".stripMargin)
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val session: SparkSession = SparkSession.builder().appName("ZoneAnys").master("local[*]").getOrCreate()
    val df: DataFrame = session.read.parquet(inputPath)
    df.createTempView("logData")
    val sql =
      """select provincename,cityname,
        |sum(case when requestMode ==1 and processNode >= 1 then 1 else 0 end) ysqq,
        |sum(case when requestMode ==1 and processNode >= 2 then 1 else 0 end) yxqq,
        |sum(case when requestMode ==1 and processNode == 3 then 1 else 0 end) ggqq,
        |sum(case when adplatformproviderid >=100000 and iseffective == 1 and isbilling ==1 and isbid ==1 and adorderid != 0 then 1 else 0 end) cyjj,
        |sum(case when adplatformproviderid >=100000 and iseffective == 1 and isbilling ==1 and iswin ==1 then 1 else 0 end) jjcg,
        |jjcg/cyjj as jjcgl,
        |sum(case when requestmode == 2 and iseffective == 1 then 1 else 0 end) zss,
        |sum(case when requestmode == 3 and iseffective ==1 then 1 else 0 end) djs,
        |djs/zss as djl,
        |sum(case when adplatformproviderid >=100000 and iseffective == 1 and isbilling ==1 and iswin ==1 and adorderid >= 200000 and adcreativeid>=200000 then (adpayment*1.0/1000) else 0 end) as dspggcb,
        |sum(case when adplatformproviderid >=100000 and iseffective == 1 and isbilling ==1 and iswin ==1 and adorderid >= 200000 and adcreativeid>=200000 then (winprice*1.0/1000) else 0 end) as dspggxf
        |from logData
        |group by provincename,cityname""".stripMargin
  }

}
