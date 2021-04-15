package com.dahua5

object TeaTest {

  def main(args: Array[String]): Unit = {
    val teacher1 = new Teacher("张三",19)
    println(teacher1.name)


    // 不使用new,来创建对象。
    val tea1 = Teacher.apply("lisi",20)
    println(tea1.name)

    val tea2 = Teacher("wangwu",20)
    println(tea2.name)

    // Scala的单例
   println(SingleDemo.a)

  }

}
