package com.dahua2

/**
  * 偏函数
  * 函数中的参数类型可容纳多种类型。处理过程中，根据类型进行处理。
  *  def f3:PartialFunction[Any,String]
  *  any 是输入函数的数据类型
  *  String 进行处理的数据类型以及返回的数据类型。
  *
  * 部分参数应用函数
  * 柯里化
  */
object Partial {

  def f1(a:Any): Int ={
   if(a.equals("a"))97 else 0
  }

  // 偏函数
  def f2:PartialFunction[Any,Int]={
    case "a" => 97   // 根据值进行匹配
    case "b" => 98
    case _ => 0
  }

  // 偏函数在数组集合中，能够发挥作用。
  def f3:PartialFunction[Any,String]={
    case i:String => "["+i+"]"// 根据类型匹配
      // 模式匹配：  i:输入参数根据String类型进行判定。如果是其他类型，不处理。
  }


  def main(args: Array[String]): Unit = {
     val res0: Int = f1(9)
     println(res0)
    // 调用偏函数
    val res1 = f2(3.15)
    println(res1)

    // 定义数组
    val array: Array[Any] = Array[Any](1,2,3,4,5,"hello","scala")
    val res2: Array[String] = array.collect(f3)
    println(res2.toBuffer)
  }

}
