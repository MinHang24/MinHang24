package spark.bean

/**
 * @ClassName Mapper
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 19:27
 * @Version 1.0
 */
class Mapper(val appid:String,val appname:String) extends Product with Serializable {
  override def productElement(n: Int): Any = n match {
    case 0 => appid : String
    case 1 => appname : String
  }

  override def productArity: Int = 2

  override def canEqual(that: Any): Boolean = this.isInstanceOf[Mapper]
}
object Mapper{
  def apply(array: Array[String]): Mapper = new Mapper(array(0),array(1))
}
