package com.dahua5

/**
  * 实现特性（接口）
  */
class TraitOneImpl extends AbstractClass with TraitOne with TraitTwo{
  // 强制实现的。特性中的方法。
  override def hello(): Unit = {

    PI = 45
    println(PI)
  }

  // 强制实现的。特性中的方法。
  override def say(str: String): Unit = println("say scala 语言")


  override def play(str: String): Unit = super.play(str)

  // 特性二中的方法。
  override def show(): Unit = println("实现特性二")

  // 抽象类中的方法.
  override def eat(str: String): Unit = ???
}
