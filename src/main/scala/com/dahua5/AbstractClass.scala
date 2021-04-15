package com.dahua5

/**
  * 抽象类
  */
abstract class AbstractClass {

  // 抽象方法。
  def eat(str:String)

  // 普通方法。
  def play(str:String)={
    println("喜欢玩的游戏："+str)
  }

}
