package cn.dmp

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object ChannelDimension {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)


    val Array(inputPath,outputPath) = args
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    df.createTempView("ChannelTable")
    val df2 = sQLContext.sql(
      """
        |select adplatformkey,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as originalRequestNum,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as effectiveRequestNum,
        |sum(case when requestmode = 1 and processnode  = 3 then 1 else 0 end) as adRequestNum,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as joinBiddingNum,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as complitedBiddingNum,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as displayNum,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clickNum,
        |sum(case when iseffective = 1 and isbilling = 1 then winPrice/1000 else 0 end) as dspConsume,
        |sum(case when iseffective = 1 and isbilling = 1 then adpayment/1000 else 0 end) as dspCost
        |from ChannelTable group by adplatformkey
      """.stripMargin)
    val load: Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    //df2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)
    df2.show()
    df2.coalesce(1).write.json(outputPath)
  }
}
