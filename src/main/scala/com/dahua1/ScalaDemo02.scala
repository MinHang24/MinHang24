package com.dahua1

/**
  * 函数和方法有什么区别：
  * 在方法中，可以将函数作为参数进行传递和调用
  */
object ScalaDemo02 {

  // 创建函数  乘法
  val f1: (Int, Int) => Int = { (x, y) => x * y }
  // 创建函数  加法
  val f2: (Int, Int) => Int = { (x, y) => x + y }
  // 创建函数  减法
  val f3 = (x: Int, y: Int) => x - y

  // 定义方法，方法中，定义函数
  def cheng(f: (Int, Int) => Int, x: Int, y: Int): Int = {f(x, y)}

  /**
    * def cheng( x: Int, y: Int): Int = (执行代码)
    * def 方法名（x,y 参数）：Int 是返回值 = 具体的执行
    *
    * 希望在方法中，传入一个函数，我要执行这个函数。
    *
    * def cheng(f:(Int,Int)=>Int,x:Int,y:Int)={我要执行这个函数 f(x,y)}
    *
    */

  def main(args: Array[String]): Unit = {
    // 调用方法 执行乘法
    val res0: Int = cheng(f1, 5, 6)
    println(res0)

    // 调用方法 执行加法
    val res1: Int = cheng(f2, 5, 6)
    println(res1)

    // 调用方法 执行减法
    val res2: Int = cheng(f3, 5, 6)
    println(res2)
  }

}
