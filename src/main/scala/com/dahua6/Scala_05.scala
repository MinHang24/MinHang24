package com.dahua6

/**
 * 隐士类。
  */
object Scala_05 {

  implicit class DB1(val m:Mysql1){// 通过构造函数，传入对象。其对象，就拥有了可调用方法的能力。
    def display = println(m.n+"hello")
  }

  def main(args: Array[String]): Unit = {
    val mysql = new Mysql1("mysql")
    mysql.display
  }
}

class Mysql1(var n:String){

}


