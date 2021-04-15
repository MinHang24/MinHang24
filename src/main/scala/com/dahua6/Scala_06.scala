package com.dahua6

/**
  * 隐士函数
  *
  * 隐士类
  *
  * 隐士参数
  */
object Scala_06 {
  def main(args: Array[String]): Unit = {
     hello("siri")

  }

  // 隐士参数
  implicit val name:String = "学生"

  def hello(str:String)(implicit content:String)={
    println(s"你好：${str}"+" 职业： "+content)
  }

}
