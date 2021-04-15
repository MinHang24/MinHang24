package com.dahua6

/**
  * 隐士类
  * A  a = new A()
  * a.a()
  *
  * B  b = new B()
  * b.b()
  *
  * 我要通过怎样的方式：
  * a.a()
  * a.b()
  *
  * 隐士类增强。
  *
  */


object Scala_02 {

  // 隐士转换
  implicit def a2B(a:A):B={
    new B
  }

  def main(args: Array[String]): Unit = {
      val a = new A()

      val b = new B()


       // 我想要：a中也能调用B的方法。
      a.b("B")

  }

}

class A{
  def a(name:String)={
    println(s"我是A方法：${name}")
  }

}

class B{
  def b(name:String)={
    println(s"我是B方法：${name}")
  }

}

