package com.dahua3

import scala.collection.mutable.ArrayBuffer

/**
  * 数组：Array
  * Array创建的数组长度不可变。数组中的值可变。
  * ArrayBuffer 创建的数组长度可变。
  */
object ScalaDemo {
  def main(args: Array[String]): Unit = {
    // 创建数组
    var arr = Array(1,2,3,4,5)
    arr(0) = 100
    println(arr.toBuffer)
    println(arr(1))// 数组赋值或取值，都用（下标）方式。

    var arr1 = new Array[Int](5)
    arr1(0) = 20
    arr1(1) = 30
    println(arr1.toBuffer)

    // 有一点要注意：val 定义的数组，不能重新定义。
    val arr2 = Array(1,2,3,4,5,6,7,8,9)
//    arr2 = Array(5,6,7,8,9)// 错误
    println("=================================")
    // 遍历数组
    for(i <- 0 until(arr2.length)){
      print(arr2(i)+" ")
    }
    println("=================================")
    // 使用特定步长遍历。
    for(i <- 0 until(arr2.length,2)){
      print(arr2(i)+" ")
    }
    // 反向遍历
    println("=================================")
    for(i <- (0 until arr2.length).reverse){
      print(arr2(i)+" ")
    }
    println("=================================")
    // forEach 遍历
    for(ele <- arr2){
      print(ele+" ")
    }

    //边长数组，数组换乘类
    println("=================================")
    val ab = ArrayBuffer[Int]()
    // 向其内部插入值?
     ab+=1
     ab+=1
     ab+=1
     ab+=(2,3,4)
     //++=
     ab ++= Array(5,6,7)
    println(ab.toBuffer)
    println("数组缓冲类第5位："+ab(5))
    // 可以转换成数组
    ab.toArray
  }


}
