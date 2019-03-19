package cn.dmp.Utils.Jedis

import java.util
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object KafkaOffsetJedisUtil {
  def updateOffset(groupId:String, offsetRanges: Array[OffsetRange]): Unit = {
    val jedis: Jedis = JedisConnectionPool.getConnection()
    for(range <- offsetRanges){
      jedis.hset(groupId,range.topic+"-.-"+range.partition,range.untilOffset.toString)
    }
    jedis.close()
  }

  def getOffset(groupId: String): Map[TopicPartition, Long] = {
    var offset = Map[TopicPartition, Long]()
    val jedis: Jedis = JedisConnectionPool.getConnection()
    val groupMap: util.Map[String, String] = jedis.hgetAll(groupId)
    jedis.close()
    import scala.collection.JavaConversions._
    val groupList: List[(String, String)] = groupMap.toList
    for(every <- groupList){
      val topicPartition: Array[String] = every._1.split("-.-")
      offset += (new TopicPartition(topicPartition(0),topicPartition(1).toInt) -> every._2.toLong)
    }
    offset
  }
}
