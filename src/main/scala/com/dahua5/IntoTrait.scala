package com.dahua5

/**
  * 混入特质
  */
class Stu(){}

object IntoTrait {

  def main(args: Array[String]): Unit = {
    // 混入特质：像java中的匿名内部类。
     var stu =  new Stu() with TraitOne with TraitTwo {
        override def hello(): Unit = println("hello 混入特质")

        override def say(str: String): Unit = ???

        override def show(): Unit = ???
      }
    stu.hello()
  }
}
