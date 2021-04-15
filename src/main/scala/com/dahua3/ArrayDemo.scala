package com.dahua3

/**
  * 数组的函数练习
  */
object ArrayDemo {

  def main(args: Array[String]): Unit = {
    val arrs = Array(1, 4, 67, 89, 23, 90, 16)
    // map 方法。 对数组中所有的元素的映射。
    val res0: Array[Int] = arrs.map(x => x * 10)
    println(res0.toBuffer)
    // 数组中的聚合函数
    println(arrs.sum)
    println(arrs.max)
    println(arrs.min)
    println(arrs.count(x => x % 2 == 0))

    /**
      * map 每个元素的映射
      * flatten   扁平化。切分压平。
      * flatMap   对内部元素切分压平。
      * groupby 分组。
      * mapValues() 针对map中的value字段进行操作。
      * toList 转换成List集合。
      * sortBy 函数。进行排序。
      */
    println("====================================================")
    val strs = Array("hello world hello jack hello tom", "hello tom world world jerry")
    // 计算wordCount x = "hello world hello jack hello tom"
    val res1: Array[Array[String]] = strs.map(x => x.split(" "))
    // 希望得到  Array[String]   Array(hello world hello jack hello tom,hello tom world world jerry)
    val flatten: Array[String] = res1.flatten
    //基于以上用到了，两个函数：map  flatten  scala 考虑到写两个函数较为复杂，提出了一个函数 flatMap

    val res2: Array[String] = strs.flatMap(_.split(" "))
    // (hello, world, hello, jack, hello, tom, hello, tom, world, world, jerry)
    val res3: Map[String, Array[String]] = res2.groupBy(x => x)
    //  Map(jack -> Array(jack), world -> Array(world, world, world), tom -> Array(tom, tom), hello -> Array(hello, hello, hello, hello), jerry ->
    val res4: Map[String, Int] = res3.mapValues(x => x.length)
    // 可否进行排序。 可以。转换成List集合就能排序。
    val list: List[(String, Int)] = res4.toList.sortBy(x => -x._2)
    for (ele <- list) {
      println(ele._1 + "  " + ele._2)
    }

    println("##====================================================")
    // 简化开发流程。
    // 考试： 就这一行代码。
    // 面试题中： 你会写几种wordCount?
    strs.flatMap(_.split(" ")).groupBy(x => x).mapValues(x => x.length).toList.sortBy(x => x._2).foreach(x => println(x))


  }


}
