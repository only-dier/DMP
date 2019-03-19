package cn.dmp.Tag

import cn.dmp.Tag.utils.TagUtils
import cn.dmp.Utils.RedisUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppTags extends Tags {
  override def label(args: Any*): List[(String, Int)] = {
    val appcaseBroad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    //val keywordBlackBroad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var list: List[(String, Int)] = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    //匹配App名称 appname
    val appname = row.getAs[String]("appname")
    if(StringUtils.isNotBlank(appname)){
      val appName: String = TagUtils.matchOnBroad(appcaseBroad,appname)
      //val appName: String = RedisUtils.get(appname)
      list :+= ("APP" + appName, 1)
    }else{
      list :+= ("APPunknow" , 1)
    }
    list
  }
}
