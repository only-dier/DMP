package cn.dmp

import java.util.Properties

import cn.dmp.Utils.{DMPUtils, MyUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object AreaDimension {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val Array(inputPath,outputPath) = args
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    df.createTempView("AreaTable")
    val df2 = sQLContext.sql(
      """
        |select provincename,cityname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as originalRequestNum,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as effectiveRequestNum,
        |sum(case when requestmode = 1 and processnode  = 3 then 1 else 0 end) as adRequestNum,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as joinBiddingNum,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as complitedBiddingNum,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as displayNum,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clickNum,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then winPrice/1000 else 0 end) as dspConsume,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then adpayment/1000 else 0 end) as dspCost
        |from AreaTable group by provincename,cityname
      """.stripMargin)
    val load: Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    df2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

    /**
      * 方法二
      * */
    //val provincename = x.getAs("provincename").toString
    //      val cityname = x.getAs("cityname").toString
    val tupels =df.rdd.map(x=>{
      val provincename = x.getAs("provincename").toString
      val cityname = x.getAs("cityname").toString
      val requestmode = x.getAs("requestmode").toString.toInt
      val processnode = x.getAs("processnode").toString.toInt
      val iseffective = x.getAs("iseffective").toString.toInt
      val isbilling = x.getAs("isbilling").toString.toInt
      val isbid = x.getAs("isbid").toString.toInt
      val iswin = x.getAs("iswin").toString.toInt
      val adorderid = x.getAs("adorderid").toString.toInt
      val winprice = x.getAs("winprice").toString.toDouble
      val adpayment = x.getAs("adpayment").toString.toDouble
      val province_city = provincename +"_"+ cityname

      (province_city,requestmode,processnode,iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
    })
    val grouped: RDD[(String, Iterable[(String, Int, Int, Int, Int, Int, Int, Int, Double, Double)])] = tupels.groupBy(_._1)
    val summed = grouped.map(x=>{
      val iterator= x._2.toIterator

      var originalRequestNum = 0
      var effectiveRequestNum = 0
      var adRequestNum = 0
      var joinBiddingNum = 0
      var complitedBiddingNum = 0
      var displayNum = 0
      var clickNum = 0
      var dspConsume = 0.0
      var dspCost = 0.0

      while (iterator.hasNext){
        val tuple = iterator.next()
        if(tuple._2 == 1 && tuple._3 >= 1){
          originalRequestNum += 1
        }
        if(tuple._2 == 1 && tuple._3 >= 2){
          effectiveRequestNum += 1
        }
        if(tuple._2 == 1 && tuple._3 == 3){
          adRequestNum += 1
        }
        if(tuple._4 == 1 && tuple._5 == 1 && tuple._6 == 1){
          joinBiddingNum += 1
          if(tuple._4 == 1 && tuple._5 == 1 && tuple._7 == 1 && tuple._8 != 0){
            complitedBiddingNum += 1
            dspConsume += tuple._9/1000
            dspCost += tuple._10/1000
          }
        }

        if(tuple._2 == 2 && tuple._4 == 1){
          displayNum += 1
        }
        if(tuple._2 == 3 && tuple._3 == 1){
          clickNum += 1
        }

      }
      (x._1.split("_")(0),x._1.split("_")(1),originalRequestNum, effectiveRequestNum, adRequestNum , joinBiddingNum , complitedBiddingNum , displayNum , clickNum, dspConsume,dspCost)
    })
    summed.foreach(println)

    /**
      * Spark Core 各省市数据量分布
      * */
    import sQLContext.implicits._
    val ta = df.map(row=>{
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val matchRequestList: List[Double] = DMPUtils.matchRequest(requestmode,processnode)
      val matchBiddingList: List[Double] = DMPUtils.matchBidding(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val matchAdList: List[Double] = DMPUtils.matchAd(requestmode,iseffective)
      ((provincename,cityname),matchRequestList ++ matchBiddingList ++ matchAdList)
    })
     val result: RDD[((String, String), List[Double])] = ta.rdd.reduceByKey((list1, list2)=>{
      val t: List[(Double, Double)] = list1.zip(list2)
      t.map(x=>x._1+x._2)
    })
    result.saveAsTextFile(outputPath)
  }
}
