package com.dahua2

import java.text.SimpleDateFormat
import java.util.Date

object PrintLog {

  // 定义打印日志的方法。
  def log(f:() =>String ,msg:String)= println("时间"+f()+" 日志："+msg)



  val printTime = () =>{
    var time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    time
  }

  def main(args: Array[String]): Unit = {


     // 使用部分参数
    val res0: String => Unit = log(printTime,_:String)
    res0("jack: 登录")
    Thread.sleep(2000)
    res0("jack: 登录")
    res0("jack: 登录")
  }

}
