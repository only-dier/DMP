package cn.dmp.Tag

import ch.hsr.geohash.GeoHash
import cn.dmp.Tag.utils.BaiduLBSHandler
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TradingAreaTags extends Tags {
  override def label(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val jedis: Jedis = args(1).asInstanceOf[Jedis]
    //纬度3.86~53.55 经度73.66~135.05
    val longitude = row.getAs[String]("long").toDouble
    val latitude = row.getAs[String]("lat").toDouble


    if(longitude >= 3.86 && longitude <= 53.55 && latitude >= 73.66 && latitude <= 135.05){
      val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude,longitude,8)
      var tradingArea: String = jedis.get(geoHash)
      if(StringUtils.isBlank(tradingArea)){
        tradingArea = BaiduLBSHandler.parseBusinessTagBy(longitude.toString,latitude.toString)
        if(StringUtils.isBlank(tradingArea)) tradingArea = "none"
        jedis.set(geoHash,tradingArea)
      }
      list:+= (tradingArea,1)
    }
    list
  }
}
