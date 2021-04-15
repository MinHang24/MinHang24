package com.dahua6

/**
  * 购票系统：
  * 约束购票的人群。什么人可以买票，什么人，不可以买票。
  */

class Worker(val name:String){}
class Student2(val name:String){}// 学生
class Older(val name:String){}// 老人
class SpecialPerson(val name:String){}// 特殊人群

class TichkerSystem{
   def buy(p:SpecialPerson): Unit ={
     if(p.name.length>1){
       println("您好："+p.name+"\t恭喜您购票成功")
     }else{
       println("购票失败")
     }

   }
}

object Scala_07 {

  implicit def object2SpecialPerson(obj:Object):SpecialPerson ={
     if(obj.getClass == classOf[Student2]){
       // 判断是学生对象。
       val student: Student2 = obj.asInstanceOf[Student2]
       new SpecialPerson(student.name)
     }else if(obj.getClass == classOf[Older]){
        val older: Older = obj.asInstanceOf[Older]
       new SpecialPerson(older.name)
     }else{
       new SpecialPerson("")
     }
  }

  def main(args: Array[String]): Unit = {
     val zs = new Student2("张三")
     val old = new Older("老人")
     val work = new Worker("工作人员")

    val system = new TichkerSystem
    system.buy(zs)
    system.buy(old)
    system.buy(work)

  }




}
