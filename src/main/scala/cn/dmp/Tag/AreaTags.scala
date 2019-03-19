package cn.dmp.Tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AreaTags extends Tags {
  override def label(args: Any*): List[(String, Int)] = {
//    val appcaseBroad: Broadcast[Map[String, String]] = args(0).asInstanceOf[Broadcast[Map[String, String]]]
//    val keywordBlackBroad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var list: List[(String, Int)] = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]

    //匹配 地域标签
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(provincename) && StringUtils.isNotBlank(cityname) ) {
      list :+= ("ZP" + provincename, 1)
      list :+= ("ZC" + cityname, 1)
    }else{
      list :+= ("ZPunknow", 1)
      list :+= ("ZCunknow", 1)
    }
    list
  }
}
