package com.dahua1

import scala.collection.mutable.ArrayBuffer

/**
  * 数组，在Scala中，为不可变数组。所以，数组不能new。
  *
  */
object ScalaDemo04 {

  val f1 = (x: Int) => x * 10

  // 传入数组
  def map1(x: Array[Int]) = {
    var arr = ArrayBuffer[Int]()
    for (ele <- x) {
      println(ele)
      arr += (ele * 10) // java中的append
    }
    // 返回一个数组。数组中的值，都乘以10
    arr.toArray
  }

  // 编写函数，完成数组中的值乘以10输出。  考虑：如果我不用ArrayBuffer。就用x: Array[Int]能不能得到结果。
  val f2: (Array[Int]) => Array[Int] = { x =>
    var arr = ArrayBuffer[Int]()
    for (ele <- x) {
      arr += ele * 10
    }
    arr.toArray
  }

  // 编写匿名函数
   var f3 = (x: Array[Int]) => {
    var arr = ArrayBuffer[Int]()
    for (ele <- x) {
      arr += ele * 10
    }
    arr.toArray
  }


  def main(args: Array[String]): Unit = {
    // 数组
    var arrs = Array(1, 2, 3, 4, 5)
    // 将数组中的每个元素乘以10
    val res0: Array[Int] = arrs.map(f1)
    println(res0.toBuffer)
    println("================================")
    val res1: Array[Int] = arrs.map((x: Int) => x * 10)
    println(res1.toBuffer)
    println("================================")
    val res2: Array[Int] = arrs.map(_ * 10)
    println(res2.toBuffer)
    println("================================")
    val res3: Array[Int] = arrs.map(f1(_))
    println(res3.toBuffer)

    // 我们能否自己写一个和map类似的方法。 传入每个元素。 返回值是数组，并且，每个元素的值都乘以10
    val res5: Array[Int] = map1(arrs)
    println(res5.toBuffer)


  }

}
