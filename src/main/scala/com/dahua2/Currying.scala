package com.dahua2

/**
  * 柯里化函数
  */
object Currying {

  def add(x: Int, y: Int) = x + y

  def add1(a: Int)(b: Int) = a + b

  // 柯里化函数定义
  def klh(a:Int)=(b:Int) => a+b

  def main(args: Array[String]): Unit = {
      val res0: Int = add(4,5)
      println(res0)

      val res1: Int => Int = add1(3)
      val res2: Int = res1(5)
      println(res2)

    val res3: Int => Int = klh(2)
    val res4: Int = res3(4)
    println(res4)


  }

}
