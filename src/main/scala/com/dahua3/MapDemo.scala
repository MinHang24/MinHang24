package com.dahua3

/**
  * Map key value 映射集合。  不可变。
  *
  * import scala.collection.mutable.Map  可变集合。
  */


object MapDemo {

  def main(args: Array[String]): Unit = {
    val map: Map[String, Int] = Map[String, Int]("a" -> 1, "blue" -> 4, "hello" -> 5, "sql" -> 3)
    // 获取集合中的元素。getorElse  查询不到返回默认值
    val res0: Int = map.getOrElse("aa", 0)
    println(res0)
    //  获取集合中的元素
    val value: Int = map.get("a").get
    println(value)

    // 遍历map
    val keys: Iterable[String] = map.keys
    for (elem <- keys) {
      print("\tkeys:" + elem + "\tvalue:" + map.getOrElse(elem, 0))
    }
    println("==================================")
    val values: Iterable[Int] = map.values
    values.foreach(x => print(" " + x))

    println("==================================")
    // 使用元组形式遍历
    for (e <- map) {
      println("key:" + e._1 + "\t" + e._2)
    }

    // 向map中插入值。
    val newMap: Map[String, Int] = map.++(Map("c" -> 3))
    newMap.foreach(println)
    println("======================================")
    // 向map拼接元素
    val newMap1: Map[String, Int] = map.+("d" -> 5)
    newMap1.foreach(println)

    println("======================================")
    // map中的size，count
    println("map集合长度：" + map.size)
    // 进行任意统计
    // Map[String,Int]("a" -> 1,"blue" -> 4,"hello" -> 5,"sql" -> 3)
    println("包含L的key有：" + map.count(_._1.contains("l")) + "个")
    println("偶数value有：" + map.count(_._2 % 2 == 0) + "个")

    // filter

    // exit


  }

}
