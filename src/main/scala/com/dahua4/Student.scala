package com.dahua4

/**
  *
  * Scala中对象分为两种：
  * 1： Object 是静态类。 类中定义的属性和方法都是静态的。
  *
  * 2： class  类：具有属性，方法，构造方法。使用该类需要new
  *
  */
object Student {
  // 属性
  var name: String = "张三"
  var age: Int = 19

  // 方法
  def show(): Unit = {
    println(s"姓名：${name}" + s" 年龄：${age}")
  }

  // 是否具有构造方法。
  // apply()
  def apply(str: String,num:Int): Unit = {
    name = str
    age = num
  }

  // 允许多个apply方法存在。
  def apply(str: String,num:Int,a:Int): Unit = {
    name = str
    age = num
  }

}
