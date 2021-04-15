package com.dahua6

/**
  * 隐士转换，增强类型。
  */
object Scala_03 {

  implicit def mysql2DB(ms:MySql):DB=new DB
  implicit def mysql2Nosql(ms:MySql):NoSql=new NoSql

  def main(args: Array[String]): Unit = {
    val mysql = new MySql
    mysql.insert()
    mysql.select()

    // mysql  想要创建数据。mysql没有方法，而DB有。
    mysql.create()
    mysql.createTable
    mysql.keyVal

  }

}

class MySql{
  def select(): Unit ={
    println("我能够进行查询……")
  }
  def insert(): Unit ={
    println("我能够进行插入……")
  }

}

class DB{
   def create(): Unit ={
     println("我能创建数据库")
   }
  def createTable: Unit ={
    println("我能建表")
  }
}

class NoSql{
    def keyVal: Unit ={
      println("转换成key Value形式")
    }
}

