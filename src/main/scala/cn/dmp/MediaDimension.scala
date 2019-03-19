package cn.dmp

import java.util.Properties

import cn.dmp.Utils.DMPUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable

object MediaDimension {
  def main(args: Array[String]): Unit = {
    val Array(inputPath,outputPath,appCasePath) = args
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val files: RDD[String] = sc.textFile(appCasePath)
    val filtered: RDD[Array[String]] = files.map(_.split("\t",-1)).filter(_.length>=5)
//    var map = scala.collection.mutable.Map[String,String]()
//    val tuples = filtered.foreach(x=>{
//      map += x(4)->x(1)
//    })
    val map: Map[String, String] = filtered.map(x=>{(x(4),x(1))}).collect().toMap
    //将map存入redis
    DMPUtils.addAppToRedis(map)
    //将map进行广播
    val broad: Broadcast[Map[String, String]] = sc.broadcast(map)

    val df: DataFrame = sQLContext.read.parquet(inputPath)
        df.createTempView("MediaTable")

        sQLContext.udf.register("matchApp",(x:String)=>{
          val value: Map[String, String] = broad.value
          val str: String = value.getOrElse(x,x)
          str
        })

        val df2 = sQLContext.sql(
          """
            |select matchApp(appname),
            |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as originalRequestNum,
            |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as effectiveRequestNum,
            |sum(case when requestmode = 1 and processnode  = 3 then 1 else 0 end) as adRequestNum,
            |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as joinBiddingNum,
            |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as complitedBiddingNum,
            |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as displayNum,
            |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clickNum,
            |sum(case when iseffective = 1 and isbilling = 1 then winPrice/1000 else 0 end) as dspConsume,
            |sum(case when iseffective = 1 and isbilling = 1 then adpayment/1000 else 0 end) as dspCost
            |from MediaTable group by appname
          """.stripMargin)

        val load: Config = ConfigFactory.load()
        val properties = new Properties()
        properties.setProperty("user",load.getString("jdbc.user"))
        properties.setProperty("password",load.getString("jdbc.password"))
    //df2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)
    df2.coalesce(1).write.json(outputPath)
  }



}
