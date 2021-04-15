package com.dahua2

/**
  * 部分参数引用函数
  */
object ScalaDemo01 {

  def add(a: Int, b: Int) = a + b

  val f1: (Int, Int) => Int = { (x, y) => x + y }

  val f2 = (x: Int, y: Int) => x + y

  def main(args: Array[String]): Unit = {
    val res0: Int = add(3,5)
    println(res0)
    // 部分参数引用， 我暂时只知道一个数字，不知道第二个数字。
    val res1: Int => Int = add(3,_:Int)
    val res2: Int = res1(10)
    println(res2)

  }

}
