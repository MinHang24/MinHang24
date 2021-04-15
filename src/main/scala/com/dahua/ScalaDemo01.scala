package com.dahua

import java.text.SimpleDateFormat

/**
  *
  */
object ScalaDemo01 {

  def main(args: Array[String]): Unit = {
    // scala语法。
    /*
       定义变量

       var  a = 10   定义变量，变量可变
       val  a = 10   定义变量，变量不可变   final
     */
     var  name  = "张三"
     println(name)
     name = "李四"
     println(name)

     val age = 18
//     age = 19  不可修改变量值。

    println("======================================")

    /**
      * 输出类型
      * println
      * printf()
      * println(s)
      */
    var stuName = "jack"
    var stuAge = 19
    var stuHige = 170.5d
    println("hello"+stuName)
    println(f"姓名：${stuName}",s"年龄：${stuAge}",s"身高：${stuHige}")
    printf("姓名：%s,年龄：%s,身高，%s",stuName,stuAge,stuHige)
    // 初始化对象
    println("======================================")
    var student = new Student("tom",20)
    println(s"姓名：${student.name}")

    println("======================================")
    // 问题：字符串和基本数据类型转换。可以使用Java的API.
    var num = "10"
    var num1 = 20
    var sum = Integer.parseInt(num)+num1

    println(sum)
  }

}

/*
  定义类
 */
// 有参构造方法
class Student(var name:String,var age:Integer)
