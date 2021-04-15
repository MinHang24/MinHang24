package com.dahua

/**
  * for 循环
  */
object ScalaDemo04 {

  def main(args: Array[String]): Unit = {
      // for 循环语法。
      for(ele <- 1 to 10 ){// 和shell的语法相似
        print(ele+" ")
      }
     println("+=============================")
    // Until 语法 for循环
    for(a <- 1 until(10))
      print(a+" ")

    println("+=============================")
    // 定义数组
    val ints = Array(3,4,67,90,22,75)//
    for(ele <- ints)
      print(ele+" ")// 相当于forEach 循环

    println("+=============================")
    for(ele <- 0 until(ints.length))
      print(ints(ele)+" ")

    println("+=============================")
    // 需求：显示出所有的偶数。  给 for循环添加守卫。
    for(ele <- 0 until(ints.length)  if(ints(ele)%2==0))
     print(ints(ele)+" ")

    println("+=============================")
    // 需求：执行for循环。将 偶数 循环后添加到新的数组中。
    var arr2 = for(e <- ints if(e%2==0))
      yield e
    for(e <- arr2)print(e+" ")
    println("+=============================")
    // 需求： 嵌套循环
    for(i <- 1 to 10 ;j <- 1 to 5 ; k <- 1 to 3)
      println(i+"   "+j+"   "+k)

    // 作业：  打印99乘法表
    // 作业：  冒泡算法
    // 预习：  scala 中的方法和函数 先进行预习。
  }

}
