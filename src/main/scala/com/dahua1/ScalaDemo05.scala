package com.dahua1

/**
  * 多参数函数
  */
object ScalaDemo05 {

  def m1(x: Int*) = {
    var sum = 0
    for (ele <- x) {
      sum += ele
    }
    sum
  }

  def main(args: Array[String]): Unit = {
     val sum: Int = m1(1,2,3,4,5)
     println(sum)
  }

}
