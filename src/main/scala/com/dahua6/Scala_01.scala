package com.dahua6

/**
  * 隐士转换函数
  */
object Scala_01 {

  // 隐士转换函数
  // Double==>Int

  implicit def dou2Int(d:Double):Int={
    d.toInt
  }
// 隐士函数不能有相同参数和相同的返回值类型。会让scala无法精确进行转换。
//  implicit def Idou2Int(d:Double):Int={
//    d.toInt
//  }// 错误

  implicit def str2Int(s:String):Int={
    Integer.parseInt(s)
  }

  def main(args: Array[String]): Unit = {
    var num:Int = 3.15
    println(num)
    var num1:Int = "1234"
    println(num1)

  }

}
