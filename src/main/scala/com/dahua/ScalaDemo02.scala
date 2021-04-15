package com.dahua

/**
  * 运算符
  * 和java相同，但也有不同
  * 可以使用java中的算数运算符进行计算，换算成scala:
  * .+()   .-()   .*()   ./()   .%()
  * 比较运算符
  * 自增自减运算符  ++  --  不存在。   替代是：  +=  -=  运算符结果无返回值，变量自增。
  * 逻辑运算符
  *
  *
  *
  */
object ScalaDemo02 {

  def main(args: Array[String]): Unit = {
    // 例如使用java中算数运算符
    var a = 10
    var b = 3

    // 进行加法运算符
    var c = a+b
    println(c)
    println("==========================")
    // 在scala中，算数运算符都是函数的表现： +  和   .+()   表达同一个意思即加法。
    var d = a.+(b)
    println(d)

    val i: Int = a.*(10).+(15)
    println(i)
    println("=========================")
    // 问题：自增自减运算符java 和 scala 各自的编写方式：
    var j  = 10
    j+=1
    println(j)
    println("=========================")
    // 比较运算符  逻辑运算符
    var num1 = 10
    var num2 = 20
    println(num1>num2 || num1<num2)



  }

}
