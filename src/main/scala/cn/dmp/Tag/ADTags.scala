package cn.dmp.Tag

import cn.dmp.Tag.utils.TagUtils
import cn.dmp.Utils.RedisUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

//广告位标签
object ADTags extends Tags {
  //
  override def label(args: Any*): List[(String, Int)] = {

//    val appcaseBroad: Broadcast[Map[String, String]] = args(0).asInstanceOf[Broadcast[Map[String, String]]]
//    val keywordBlackBroad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var list: List[(String, Int)] = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]

    //匹配 	广告位类型 adspacetype
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case ty if ty > 9 => list :+= ("LC" + ty, 1)
      case ty if ty > 0 && ty <= 9 => list :+= ("LC0" + ty, 1)
    }

    //匹配 	广告位类型名称 adspacetypename
    val adTypeName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adTypeName)) {
      list :+= ("LN" + adTypeName, 1)
    } else {
      list :+= ("LNunknow", 1)
    }
    list
  }
}
