package spark.Utils

/**
 * @ObjectName GetID
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/14 9:55
 * @Version 1.0
 */
object GetID {

  def get(array: Array[String]):String={
    if(array(47).nonEmpty){
      array(47)
    }else if(array(48).nonEmpty){
      array(48)
    }else if(array(49).nonEmpty){
      array(49)
    }else if(array(50).nonEmpty){
      array(50)
    }else if(array(51).nonEmpty){
      array(51)
    }
  }

}
