package cn.dmp.Tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object DeviceTags extends Tags {
  override def label(args: Any*): List[(String, Int)] = {

//    val appcaseBroad: Broadcast[Map[String, String]] = args(0).asInstanceOf[Broadcast[Map[String, String]]]
//    val keywordBlackBroad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var list: List[(String, Int)] = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]

    //匹配 操作系统 client
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list :+ ("Android D00010001", 1)
      case 2 => list :+ ("IOS D00010002", 1)
      case 3 => list :+ ("WinPhone D00010003", 1)
      case _ => list :+ ("其 他 D00010004", 1)
    }

    //匹配 联网方式名称 networkmannername
    val networkmannername = row.getAs[String]("networkmannername")
    networkmannername match {
      case "WIFI" => list :+= ("WIFI D00020001", 1)
      case "4G" => list :+= ("4G D00020002", 1)
      case "3G" => list :+= ("3G D00020003", 1)
      case "2G" => list :+= ("2G D00020004", 1)
      case _ => list :+= ("其 他 D00020005", 1)
    }

    //匹配 设备运营商方式 ispname
    val ispname = row.getAs[String]("ispname")
    ispname match {
      case "移动" => list :+= ("移动 D00030001", 1)
      case "联通" => list :+= ("联通 D00030002", 1)
      case "电信" => list :+= ("电信 D00030003", 1)
      case _ => list :+= ("其 他 D00030004", 1)
    }
    list
  }
}
