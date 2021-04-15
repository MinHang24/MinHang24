package com.dahua4

class TeaDemo private (var n:String,var y:Int){


}

object TeaDemo{
  def apply(n:String,y:Int):TeaDemo={
    new TeaDemo(n,y)
  }

  def main(args: Array[String]): Unit = {
    val teaDemo = TeaDemo("",9)

  }
}
