package com.dahua4

/**
  * 测试类
  */
object Test {

  def main(args: Array[String]): Unit = {
    println(Student.name)// Object定义的类是：静态类。属性是静态属性。

    Student.show()// 调用静态方法。

    // 改变对象中属性的值：
    Student.name = "李四"
    println(Student.name)

    println("+=========================================")
    // 调用对象的apply方法。
    Student.apply("糖果",10)// 语法糖

    println(Student.name)
    println(Student.age)

    println("+=========================================")

    Student("糖果1",100)// 语法糖
    println(Student.name)
    println(Student.age)

    // 可以参照以下类。
    List(1,2,3,4)
    List.apply(1,2,3,4)





  }

}
