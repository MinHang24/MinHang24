package spark.logAnlyse

import spark.bean.ProvincePartition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName ProCityCountSort
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/12 16:28
 * @Version 1.0
 */
object ProCityCountSort {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("""
                |缺少参数
                |inputPath
                |outputPath""".stripMargin)
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName("ProCityCount")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile(inputPath)
    val rdd2: RDD[Array[String]] = rdd1.map((line: String) => {
      val strs: Array[String] = line.split(",", -1)
      strs
    })
    val etl: RDD[Array[String]] = rdd2.filter(_.length >= 85)
    val data: RDD[((String, String), Int)] = etl.map((strs: Array[String]) => ((strs(24), strs(25)), 1))
    val rdd3: RDD[((String, String), Int)] = data.reduceByKey(_ + _).sortBy(_._2)
    val mapper: RDD[(String, (String, Int))] = rdd3.map(line => {
      (line._1._1, (line._1._2, line._2))
    })
    //自定义分区   排序
    val rdd4: RDD[(String, (String, Int))] = mapper.partitionBy(new ProvincePartition(mapper.keys.collect().toSet))

    val config: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(config)
    val path = new Path(outputPath+"_proSort")
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    rdd4.saveAsTextFile(outputPath+"_proSort")
    sc.stop()
  }
}
