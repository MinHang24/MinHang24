package com.dahua3

/**
  * List 集合在Scala 中 Seq
  * 集合：
  * ：mutable:包中的集合是可变集合。
  * 表示集合：长度可变：值可变。
  *scala.collection.mutable.ListBuffer
  * *
  * immutable: 中的是不可变集合。
  * 表示集合有序：长度不可变。 值不可变。 和Java有很大的不同。
  *scala.collection.immutable List
  *
  */
object ListDemo {

  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4) // 以后 大家就明白，因为这个List集合，会是《Kafka中的消息队列》
    // 获取集合中的元素。
    println(list(0))
    println(list(1))
    // 向其内部添加元素：
    //    list(0) = 5//错误，不能 修改，或添加集合中的值。
    // 遍历集合中的元素。
    for (ele <- 0 until (list.size)) print(list(ele) + " ")
    println("====================")
    // 获取集合头部元素
    println("头部元素：" + list.head)
    // 获取尾部元素
    println("尾部元素" + list.tail.toBuffer)
    // 使用数据拼接创建 序列  ::  ::  拼接序列
    val res0: List[Int] = 4 :: 5 :: 6 :: 7 :: Nil
    println(res0.toBuffer)

    // ++  合并序列到尾部。
    val res1: List[Int] = list.++(res0)
    println(res1.toBuffer)
    // 原集合会变吗？
    println(list.toBuffer)

    // ++:  添加集合到新集合的头部。
    val res2: List[Int] = list.++:(res0)
    println(res2.toBuffer)

    // +： 添加元素到头部
    val res3: List[Int] = list.+:(9)
    println(res3.toBuffer)

    // :+ 添加元素到尾部
    val res4: List[Int] = list.:+(9)
    println(res4.toBuffer)

    // ::: 拼接集合。
    val ints: List[Int] = list ::: List(5, 5, 5, 5)
    println(ints.toBuffer)


    // :: 拼接 元素。
    val res6: List[Any] = list :: List(5, 6, 7, 8)
    println(res6.toBuffer)

    // ::  元素拼接集合
    val res7: List[Int] = 5 :: list
    println(res7.toBuffer)
    val res8: List[Int] = list.::(5)
    println(res8.toBuffer)

    // list集合中的方法。
    // site 集合长度
    println("list集合个数：" + list.size)

    // count  统计个数
    println("偶数个数："+list.count(x => x % 2 == 0))

    // filter 过滤的方法。  元组
    val list1:List[(String,Int)]= List(("hello",12),("world",6),("tom",4))
    // 过滤String 包含l的元素。
    val tuples: List[(String, Int)] = list1.filter(x=>x._1.contains("l"))
    tuples.foreach(println)


  }

}
