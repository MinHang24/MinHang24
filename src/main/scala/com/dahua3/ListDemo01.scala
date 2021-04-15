package com.dahua3

import scala.collection.mutable.ListBuffer

object ListDemo01 {

  def main(args: Array[String]): Unit = {
    val list = List.apply(3, 67, 44, 2, 5, 90, 100)


    // filter 过滤的方法。  元组
    val list1: List[(String, Int)] = List(("hello", 12), ("world", 3), ("tom", 22), ("hello", 55))
    // 过滤String 包含l的元素。
    //    val tuples: List[(String, Int)] = list1.filter(x=>x._1.contains("o"))
    val tuples: List[(String, Int)] = list1.filter(x => x._2 < 10)
    tuples.foreach(println)

    // 排序：sorted
    val res0: List[Int] = list.sorted
    res0.foreach(x => print(x + " "))
    println("================================")
    // 排序：sortBy
    val res1: List[(String, Int)] = list1.sortBy(x => -x._2)
    res1.foreach(println)

    // fold： 折叠
    val list2 = List(1, 2, 3, 4)
    // （（（（5+1）+2）+3）+4）
    println(list2.fold(5)((x, y) => x + y))
    // (1-1)-2)-3)-4
    println(list2.foldLeft(1)((x, y) => x - y))
    // (1+4)+3)+2)+1
    println(list2.foldRight(1)((x, y) => x + y))
    // 1-(2-(3-(4-5)
    println(list2.foldRight(5)((x, y) => x - y))

    // 分组统计： 方法中定义几个数值为一组。
    val res3: List[List[Int]] = list.grouped(3).toList
    res3.foreach(println)
    //(("hello",12),("world",3),("tom",22),("hello",55))
    val map: Map[String, List[(String, Int)]] = list1.groupBy(x => x._1)
    for (ele <- map) {
      println("key：" + ele._1 + "\tvalue:" + ele._2)
    }

    // 集合反转 reverse
    val res4: List[Int] = list.sorted.reverse
    println(res4)

    // reduce 统计。
    // "hello",12),("world",3),("tom",22),("hello",55
    println(list.reduce((x, y) => x + y))

    //    val tuples1:List[(String,Int)]= List(("hello",12),("world",3),("tom",22),("hello",55))
    //    println(tuples1.reduce((x, y) => x._2 + y._2))

    // 全集，交集，差集
    var list3  = List(1,2,3,4)
    var list4 = List(3,4,5,6)
    val res5: List[Int] = list3.union(list4)// 全集
    println(res5)
    println(list3.intersect(list4))// 交集
    println(list4.diff(list3))// 差集

    // 拉链  zip
    var list5 = List(44,3,67,90)
    var list6 = List(12,6,89,71,88,71)
    val res6: List[(Int, Int)] = list5.zip(list6)
    println(res6)

    // 局部聚合，全局聚合  spark。 想想一下：combiner     reduce
    // 12,6,89,71,88,71)
    println(list.aggregate(1)((x, y) => x + y, (x, y) => x + y))

    // 集合中，长度。
    println("集合长度："+list.length)

    // sortWith 根据什么排序。
    val res7: List[Int] = list.sortWith((x,y)=>x>y)
    println(res7)

    // 可变集合。
    val res8: ListBuffer[Int] = ListBuffer[Int]()// 和java中的ArrayList相似。
    res8+=12
    res8+=58
    res8+=15
    res8.append(30)
    res8.insert(2,1,2,3)
    println(res8.toBuffer)


  }

}
