package spark.logAnlyse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * @ObjectName ProCityCount
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/12 15:49
 * @Version 1.0
 */
object ProCityCount {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("""
                |缺少参数
                |inputPath
                |outputPath""".stripMargin)
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName("ProCityCount")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = session.sparkContext


    val df: DataFrame = session.read.parquet(inputPath)
    df.createTempView("logData")
    //统计各省市分布情况并排序
    var sql = "select provincename,cityname,count(*) as pcount from logData group by provincename,cityname order by pcount desc"
    val df1: DataFrame = session.sql(sql)
    val ds: Dataset[Row] = df1.repartition(1)
    val config: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(config)
    val path = new Path(outputPath+"proCity")
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    ds.write.json(outputPath+"proCity")
//    val in: InputStream = this.getClass.getClassLoader.getResourceAsStream("application.conf")
//    val load: Config = ConfigFactory.load()
//    val properties = new Properties()
//    properties.setProperty("driver",load.getString("jdbc.driver"))
//    properties.setProperty("user",load.getString("jdbc.user"))
//    properties.setProperty("password",load.getString("jdbc.password"))
//    df1.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName")+"_proCity",properties)

    //统计各个省份分布情况，并排序。
    var sql1 = "select provincename,count(*) as pcount from logData group by provincename order by pcount desc"
    val df2: DataFrame = session.sql(sql1)
    val ds1: Dataset[Row] = df2.repartition(1)
    val path1 = new Path(outputPath+"pro")
    if(fs.exists(path1)){
      fs.delete(path1,true)
    }
    ds1.write.json(outputPath+"pro")
//    df2.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName")+"_pro",properties)

    //完成按照省分区，省内有序。
//    var sql2 = "select provincename,cityname,count(*) over(partition by provincename) from logData"
//    val df3: DataFrame = session.sql(sql2)
////    df3.write.json(outputPath+"proCitySort")
//    df3.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName")+"_proCitySort",properties)
  }

}
