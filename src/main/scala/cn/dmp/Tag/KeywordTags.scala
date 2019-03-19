package cn.dmp.Tag

import cn.dmp.Tag.utils.TagUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object KeywordTags extends Tags {
  override def label(args: Any*): List[(String, Int)] = {

    //val appcaseBroad: Broadcast[Map[String, String]] = args(0).asInstanceOf[Broadcast[Map[String, String]]]
    val keywordBlackBroad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var list: List[(String, Int)] = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    //匹配 关键字 keywords
    val keywords = row.getAs[String]("keywords")
    if (StringUtils.isNotBlank(keywords)) {
      val strings: Array[String] = keywords.split("\\|")
      for (s <- strings) {
        if( s.length >= 3 && s.length <= 8 && !TagUtils.matchOnBroad(keywordBlackBroad,s).equals("")){
          list :+= ("K" + s, 1)
        }else{
          list :+= ("Kwrong", 1)
        }
      }
    }else{
      list :+= ("Kwrong", 1)
    }
    list
  }
}
