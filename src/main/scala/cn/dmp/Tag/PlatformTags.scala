package cn.dmp.Tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object PlatformTags extends Tags {
  override def label(args: Any*): List[(String, Int)] = {
//    val appcaseBroad: Broadcast[Map[String, String]] = args(0).asInstanceOf[Broadcast[Map[String, String]]]
//    val keywordBlackBroad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var list: List[(String, Int)] = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    //匹配 渠道 adplatformproviderid
    val adPlatform = row.getAs[Int]("adplatformproviderid")
    list :+= ("CN" + adPlatform, 1)
    list
  }
}
