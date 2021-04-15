package com.dahua5
// 伴生类
class Teacher  (var name:String,var age:Int) {

}

// class  和   Object 进行整合。
// 伴生对象。
object Teacher{
  def apply(n:String,y:Int):Teacher ={// 伴生方法。
    new Teacher(n,y)
  }
}
