package com.dahua1

/**
  * 函数的传递
  */
object ScalaDemo03 {
  /**
    * 花钱和数钱
    */
  var money = 50

  // 花钱
  def huaqian() = {
    money = money - 5
  }

  // 数钱
  def shuqian() = {
    huaqian
    money
  }

  // 关于值传递
  def printByVal(x: Int) = {
    for (ele <- 0 until (3)) {
      println(s"余额：${x}")
    }
  }

  // 关于函数传递 f:() => Int
  def printByFun(x: => Int) = {
    for (ele <- 0 until (3)) {
      println(s"余额：${x}")
    }
  }


  def main(args: Array[String]): Unit = {
    // 值传递方法的调用  在参数中，传递的是值。
    printByVal(shuqian)
    println("=====================")
    // 在方法中传递函数，传递的是函数的执行体。
    printByFun(shuqian)
  }

}
