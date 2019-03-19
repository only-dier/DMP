package cn.dmp.Tag


import cn.dmp.SparkAndHBase.SparkWriteHbase
import cn.dmp.Tag.utils.{BaiduLBSHandler, TagUtils}
import cn.dmp.Utils.Jedis.JedisConnectionPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import redis.clients.jedis.Jedis


object ContextTag {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val Array(inputPath,outputPath,appCasePath,blackListPath) = args

    val appcaseFiles: RDD[String] = sc.textFile(appCasePath)
    val appcaseFiltered: RDD[Array[String]] = appcaseFiles.map(_.split("\t",-1)).filter(_.length>=5)
    //将appcase进行广播
    val appcaseMap: Map[String, String] = appcaseFiltered.map(x=>{(x(4),x(1))}).collect().toMap
    val appcaseBroad: Broadcast[Map[String, String]] = sc.broadcast(appcaseMap)
    //关键字黑名单 广播变量
    val keywordBlackList: RDD[String] = sc.textFile(blackListPath)
    val keywordBlackMap: Map[String, String] = keywordBlackList.map(_.split("\n")).map(x=>(x(0),"")).collect().toMap
    val blackbroad: Broadcast[Map[String, String]] = sc.broadcast(keywordBlackMap)
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //
//    val dffiltered1 = df.select("leng","lat").filter(
//      """
//        |cast(leng as Double) >= 73.66 and cast(leng as double) <= 135.05 and cast(lat as Double) >= 3.86 and cast(lat as double) <= 53.55
//      """.stripMargin).distinct()

//      .foreachPartition(r=>{
//        val jedis = JedisConnectionPool.getConnection()
//        r.foreach(row=>{
//          //纬度3.86~53.55 经度73.66~135.05
//          val longitude = row.getAs[String]("leng").toDouble
//          val latitude = row.getAs[String]("lat").toDouble
//          val tradingArea: String = BaiduLBSHandler.parseBusinessTagBy(longitude.toString,latitude.toString)
//          val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude,longitude,8)
//          jedis.set(geoHash,tradingArea)
//        })
//      }
//    )
    //过滤 去除没有任何唯一标识的用户
    val filted = df.filter(TagUtils.AnyUserId)
    //import sQLContext.implicits._
    val labled = filted.rdd.map(row=>{
      //进行打标签
      //获取用户所有ID
      val userAllId: List[String] = TagUtils.getAllUserId(row)
      //1)	广告位 2)	App 3)	渠道 platform 4)	设备 5)	关键字 6)	地域标签 7)	商圈标签
      val adTags: List[(String, Int)] = ADTags.label(row)
      val appTags: List[(String, Int)] = AppTags.label(row,appcaseBroad)
      val platformTags: List[(String, Int)] = PlatformTags.label(row)
      val deviceTags: List[(String, Int)] = DeviceTags.label(row)
      val keywordTags: List[(String, Int)] = KeywordTags.label(row,blackbroad)
      val areaTags: List[(String, Int)] = AreaTags.label(row)

      val jedis: Jedis = JedisConnectionPool.getConnection()
      val tradingArea: List[(String, Int)] = TradingAreaTags.label(row,jedis)
      jedis.close()
      // 先把标签加到一起
      val tags = adTags ++ appTags ++ platformTags ++ deviceTags ++ keywordTags ++ areaTags
      (userAllId,row,tags)
    })

    val value: RDD[(VertexId, List[(String, Int)])] = TagUtils.graph(labled)
    val result = value.map(x=>{
      (x._1.toString,x._2)
    })
    //按key聚合，获得（id，list((),(),()) list((),(),()) ）
//    val reduceed: RDD[(String, List[(String, Int)])] = labled.rdd.reduceByKey(
//      //（id，list((),(),()))  (id, list((),(),()) ）
//      (list1,list2)=>{
//        //（id，list((),(),() ... (),(),()))  groupby -> （id，list((),(),() ... (),(),())) -> mapValue
//        (list1:::list2).groupBy(_._1)
//          .mapValues(_.foldLeft(0)(_ + _._2)).toList
//    })
//    reduceed.foreach(println)
//    val result: RDD[String] = reduceed.map(x=>{
//      x._1 +","+ x._2.map(y=>y._1+":"+y._2).mkString(",")
//    })

    //数据存入hbase
    SparkWriteHbase.writer(result,"dmp02")
    //数据存入hdfs
    //result.saveAsTextFile(outputPath)
    //result.foreach(println)
  }
}
