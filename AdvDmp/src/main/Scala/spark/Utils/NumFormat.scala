package spark.Utils

/**
 * @ObjectName NumFormat
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/12 15:39
 * @Version 1.0
 */
object NumFormat {

  def toInt(str:String):Int={
    try{
      str.toInt
    }catch{
      case _:Exception => 0
    }
  }

  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch{
      case _:Exception => 0
    }
  }


}
