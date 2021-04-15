package com.dahua1

//写的很好
/**
  * scala中有方法和函数
  * 方法：method。
  * def 方法名（参数列表）：返回值 ={ 执行体}
  *
  * 函数：function
  * val f1:(Int,Int) => Int ={(x,y)=>x+y}
  * val 函数名：(参数数据类型)=>返回值类型={(参数命名)=>函数执行体}
  *
  * 一定要看懂这样的代表表达的意思：
  * res0:（Int,Int）=> Int = （funciton）
  * （Int,Int）=> Int
  *
  * 方法可以转换成函数，函数也可以执行和方法相同的功能。
  *
  *
  */
object ScalaDemo01 {

  // 无参，无返回值方法
  def m1(): Unit = {
    println("method1")
  }

  // 有参，有返回值方法。
  def m2(a: Int, b: Int): Int = {
    println("方法具体要干点啥……，执行业务。")
    a + b
  }

  // 方法的简写。不设定返回值类型，由方法自动推算。
  def m3(x: Int, y: Int) = {
    "string"
  }

  // 函数
  // val 函数名：(参数数据类型)=>返回值类型={(参数命名)=>函数执行体}
  val f1: (Int, Int) => Int = { (x, y) => x + y }

  // 函数简化模式
  val f2 = (x: Int, y: Int) => x + y


  def main(args: Array[String]): Unit = {
    // 调用方法1。
    m1
    // 调用方法2。
    val res0: Int = m2(3, 5)
    println(res0)

    // 调用方法3.
    val res1: String = m3(10, 4)
    println(res1)

    println("=====================================")
    // 调用函数f1
    val res2 = f1(4, 6)
    println("函数的执行：" + res2)

    // 调用函数f2()
    val res3 = f2(5,1)
    println(res3)

    // 一会儿再说
    val arrs = Array("1","2","3")
    val res4: Array[String] = arrs.map(x=>x+"**")
    println(res4.toBuffer)

    // 方法可以转换成函数   使用  方法名  _
    val function: (Int, Int) => String = m3 _
    val res5: String = function(4,6)
    println(res5)


  }


}
