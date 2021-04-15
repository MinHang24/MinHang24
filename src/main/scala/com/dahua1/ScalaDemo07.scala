package com.dahua1

/**
  * 高阶函数：函数的函数
  * 把一个函数，当做另一个函数的参数。高阶函数
  */
object ScalaDemo07 {

  def apply(f: Int => String, v: Int) = f(v)

  def layout(x: Int) = "[" + x.toString + "]"

  var f1: (Int) => String = {x => "{" + x.toString + "}"}

  def main(args: Array[String]): Unit = {
    val res0 = apply(f1, 10)
    println(res0)
  }


}
