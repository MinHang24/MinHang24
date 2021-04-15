package com.dahua3

/**
  * 元组：
  * Tuple。
  * 在scala中，包含 8种基本数据类型，引用数据类型，元组类型。
  * 只要看见括号，就是元组。
  * 访问元组中的数据  _1  _2  _3
  */
object TupleDemo {

  def add(x: Int, y: Int): (String, Int, Int, Int, Int) = {
    ("结果：", x + y, x - y, x * y, x / y)
  }

  def main(args: Array[String]): Unit = {

    val tup0: (String, Int, Boolean) = Tuple3("hello", 1, true)
    println(tup0._1)
    println(tup0._2)
    println(tup0._3)

    val tup1: (String, String, Int) = ("java", "scala", 100)
    println(tup1._1 + " " + tup1._2 + " " + tup1._3)

    println("================")
    val tup2: (String, Int, Int, Int, Int) = add(10, 3)
    println(tup2._1 + tup2._2)
    println(tup2._1 + tup2._3)

    val arr:Array[Int] = Array(1, 3, 4, 5, 6)
    var tup3 : Array[(Int,Int)] = arr.map(x=>{
      (x,x*10)
    })
    // 返回值的个数
    for(ele <- 0 until(tup3.length)){
       println(tup3(ele)._1+" "+tup3(ele)._2)
    }

    // 对偶元组
    val tup4: Array[(String, Int)] = Array(("hello",1),("scala",4))
    val swap: (Int, String) = tup4(1).swap
    println(swap._1+"   "+swap._2)
  }

}
