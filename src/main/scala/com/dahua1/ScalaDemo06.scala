package com.dahua1

/**
  * 函数的默认值
  */
object ScalaDemo06 {

  def add(x:Int=3,y:Int=5)={
    x+y
  }

  def main(args: Array[String]): Unit = {
    // 调用有默认值的方法。
    val sum: Int = add()
    println(sum)
    val res0: Int = add(10)
    println(res0)
    val res1: Int = add(y=10)
    println(res1)
    val res2: Int = add(10,20)
    println(res2)
  }

}
