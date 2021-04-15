package spark.GetLog

import spark.bean.logBean
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ObjectName logEtl
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/12 10:52
 * @Version 1.0
 */
object logEtl {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("""
          |缺少参数
          |inputPath
          |outputPath""".stripMargin)
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName("LogETL").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[logBean]))
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = session.sparkContext

    import session.implicits._
    val rdd1: RDD[String] = sc.textFile(inputPath)
    val rdd2: RDD[Array[String]] = rdd1.map(line => {
      val strs: Array[String] = line.split(",", -1)
      strs
    })
    val rdd3: RDD[Array[String]] = rdd2.filter(_.length >= 85)
    val rdd4: RDD[logBean] = rdd3.map(logBean(_: Array[String]))
    val df: DataFrame = session.createDataFrame(rdd4)
    val config: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(config)
    val path = new Path(outputPath)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    df.write.parquet(outputPath)
    session.stop()
    sc.stop()
  }

}
