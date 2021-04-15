package com.dahua4

import com.dahua5.Teacher

/**
  * Scala的普通类。
  */
class Teacher private(var n: String, var y: Int) {// n  y  已经是类中的属性了。
  // scala的有参构造方法，参数值写在类名后边

  // 属性
  var name: String = _

  var age: Int = _

  // 方法
  def show(param: String) = {
    println(param)
  }
}

  object Teacher{
    def apply(n:String,y:Int):Teacher ={// 伴生方法。
      new Teacher(n,y)
    }

}
