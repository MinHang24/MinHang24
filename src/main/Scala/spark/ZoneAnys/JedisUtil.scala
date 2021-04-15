package spark.ZoneAnys

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
 * @ObjectName JedisUtil
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 23:13
 * @Version 1.0
 */
object JedisUtil {

  val jedisPoll = new JedisPool(new GenericObjectPoolConfig, "192.168.137.90", 6379, 300000, null, 8)

  def resource: Jedis = jedisPoll.getResource

}
