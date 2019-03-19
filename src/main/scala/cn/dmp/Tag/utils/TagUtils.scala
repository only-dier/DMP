package cn.dmp.Tag.utils

import cn.dmp.Tag.ADTags
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}




object TagUtils {
  def graph(labled: RDD[(List[String], Row, List[(String, Int)])] ) = {
    //获取点
    val vd: RDD[(VertexId, List[(String, Int)])] = labled.flatMap(t=>{
      // 只有一个人携带标签数据就好，其他ID不能带，如果同一个行上的多个ID都携带了标签，
      // 标签里面的值不准确了，到最后聚合时候，数据翻倍，这样肯定不行
      val all = t._1.map((_,0)) ++ t._3
      // 我们要保证数据统一性，为了取顶点ID，所以要把所有的ID转换成HashCore值
      val result =t._1.map(uid=>{
        if(t._1.head.equals(uid)){
          (uid.hashCode.toLong,all)
        }else {
          (uid.hashCode.toLong,List.empty)
        }
      })
      result
    })
    //获取边
    val edges = labled.flatMap(t => {
      t._1.map(uid => Edge(t._1.head.hashCode, uid.hashCode.toLong, 0))
    })

    val graphX = Graph(vd,edges)
    // 取顶点
     val vert = graphX.connectedComponents().vertices
    // 聚合数据
    val result: RDD[(VertexId, List[(String, Int)])] = vert.join(vd).map{
      case (uid,(comId,tagAndUserId)) =>(comId,tagAndUserId)
    }.reduceByKey{
      case (list1,list2) =>(list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }
    result
  }


  def matchOnBroad(broad: Broadcast[Map[String, String]],s: String):String = {
    val str: String = broad.value.getOrElse(s,s)
    str
  }

  def getAnyUserId(row:Row):String  = {
    //imei: String,
    //mac: String,
    //idfa: String,
    //openudid: String,
    //androidid: String,
    //imeimd5: String,
    //macmd5: String,
    //idfamd5: String,
    //openudidmd5: String,
    //androididmd5: String,
    val result: String = row match {
      case r if StringUtils.isNotBlank(r.getAs[String]("imei")) => "IMEI:" + r.getAs[String]("imei")
      case r if StringUtils.isNotBlank(r.getAs[String]("mac")) => "MAC:" + r.getAs[String]("mac")
      case r if StringUtils.isNotBlank(r.getAs[String]("idfa")) => "IDFA:" + r.getAs[String]("idfa")
      case r if StringUtils.isNotBlank(r.getAs[String]("openudid")) => "OPENUUID:" + r.getAs[String]("openudid")
      case r if StringUtils.isNotBlank(r.getAs[String]("androidid")) => "ANDROIDID:" + r.getAs[String]("androidid")
      case r if StringUtils.isNotBlank(r.getAs[String]("imeimd5")) => "IMEIMD5:" + r.getAs[String]("imeimd5")
      case r if StringUtils.isNotBlank(r.getAs[String]("macmd5")) => "MACMD5:" + r.getAs[String]("macmd5")
      case r if StringUtils.isNotBlank(r.getAs[String]("idfamd5")) => "IDFAMD5:" + r.getAs[String]("idfamd5")
      case r if StringUtils.isNotBlank(r.getAs[String]("openudidmd5")) => "OPENUUIDMD5:" + r.getAs[String]("openudidmd5")
      case r if StringUtils.isNotBlank(r.getAs[String]("androididmd5")) => "ANDROIDIDMD5:" + r.getAs[String]("androididmd5")
      case r if StringUtils.isNotBlank(r.getAs[String]("imeisha1")) => "IMEISHA1:" + r.getAs[String]("imeisha1")
      case r if StringUtils.isNotBlank(r.getAs[String]("macsha1")) => "MACSHA1:" + r.getAs[String]("macsha1")
      case r if StringUtils.isNotBlank(r.getAs[String]("idfasha1")) => "IDFASHA1:" + r.getAs[String]("idfasha1")
      case r if StringUtils.isNotBlank(r.getAs[String]("openudidsha1")) => "OPENUUIDSHA1:" + r.getAs[String]("openudidsha1")
      case r if StringUtils.isNotBlank(r.getAs[String]("androididsha1")) => "ANDROIDIDSHA1:" + r.getAs[String]("androididsha1")
    }
    result
  }
//    if(StringUtils.isNotBlank(result)){
//      return false
//    }else{
//      return true
//    }
    val AnyUserId =
      """
        |imei !='' or mac !='' or idfa!='' or openudid !='' or androidid !='' or
        |imeimd5 !='' or macmd5 !='' or idfamd5 !='' or
        |openudidmd5 !='' or androididmd5 !='' or imeisha1 !='' or macsha1 !='' or
        |idfasha1 !='' or openudidsha1 !='' or androididsha1!=''
      """.stripMargin


  def makeLable(broad: Broadcast[Map[String, String]],blackbroad: Broadcast[Map[String, String]],row:Row): List[(String, Int)]={
    val list: List[(String, Int)] = ADTags.label(broad,blackbroad,row)
    list
  }
  def getAllUserId(row:Row):List[String] = {
    var list = List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei"))) list:+= "IMEI:" + row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac")))  list:+= "MAC:" + row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa")))  list:+= "IDFA:" + row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid")))  list:+= "OPENUUID:" + row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid")))  list:+= "ANDROIDID:" + row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5")))  list:+= "IMEIMD5:" + row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5")))  list:+= "MACMD5:" + row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5")))  list:+= "IDFAMD5:" + row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5")))  list:+= "OPENUUIDMD5:" + row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5")))  list:+= "ANDROIDIDMD5:" + row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1")))  list:+= "IMEISHA1:" + row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1")))  list:+= "MACSHA1:" + row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1")))  list:+= "IDFASHA1:" + row.getAs[String]("idfasha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1")))  list:+= "OPENUUIDSHA1:" + row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1")))  list:+= "ANDROIDIDSHA1:" + row.getAs[String]("androididsha1")
    list
  }
}
