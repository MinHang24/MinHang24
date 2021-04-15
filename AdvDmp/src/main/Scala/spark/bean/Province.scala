package spark.bean

/**
 * @ClassName Province
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/12 18:35
 * @Version 1.0
 */
class Province(provinceName:String,cityName:String,value:Int)

object Province{
  def apply(provinceName: String, cityName: String, value: Int): Province = new Province(provinceName, cityName, value)
}
