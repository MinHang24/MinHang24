package com.dahua5

/**
  * Trait是Scala中的接口。相当于java中的Interface
  */
trait TraitOne {

  // 在接口中定义属性
  var PI = 3.14


  // 接口中定义方法
  def hello()

  def say(str:String)

  // 接口中可否有普通方法
  def play(str:String)={
    println(s"喜欢玩的游戏：${str}")
  }


}
