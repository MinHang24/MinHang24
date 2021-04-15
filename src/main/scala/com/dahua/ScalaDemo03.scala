package com.dahua

/**
  * if else
  */
object ScalaDemo03 {

  def main(args: Array[String]): Unit = {
    /*
      if  else 语法
     */
    var a = 10
    println(if(a==10)"正确")
    // if else 语句
    var result= if(a%2==0)"偶数" else "奇数"
    println(result)
    // if else if 语句。
    val res1= if(a%3==0)"3的倍数" else if(a%5==0)"5的倍数"
    println(res1)
    println("=================")
    // if else if 语句。
    var res2 = if(a%3==0){
      println("3的倍数")
    }else if(a%5==0){
      100
      20
    }
      // 任何函数都可以返回值。 不需要return.也不要加return
      //  if else  的返回值，在{最后一行即为返回值}  没有{}  第一行就是返回值。
    println("if函数的返回值："+res2)
    println("============================")

    /**\
      * any  是代表scala 任意类型
      * anyVal 代表任意数值类型
      * anyRef 代表任意对象类型。
      */

    var res4:Any= if(a%3==0){
      "偶数"
    }else{
      0
    }
    println(res4)

  }

}
