package cn.dmp.Utils

import cn.dmp.Utils.Jedis.JedisConnectionPool
import redis.clients.jedis.Jedis

object RedisUtils {
  def get(key:String):String={
    val jedis: Jedis = JedisConnectionPool.getConnection()
    jedis.get(key)
  }
}
