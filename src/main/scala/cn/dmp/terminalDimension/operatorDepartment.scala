package cn.dmp.terminalDimension

import java.io

import cn.dmp.Utils.DMPUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}


object operatorDepartment {

  def groupByAndSum(tupels: RDD[(Int, Int, Int, Int, Int, Int, Int, Double, Double, String, String, String, String)],index:Int): RDD[(String, Int, Int, Int, Int, Int, Int, Int, Double, Double)] = {

    var grouped : RDD[(String, Iterable[(Int, Int, Int, Int, Int, Int, Int, Double, Double, String, String, String, String)])] = null

    if(index == 10){
      grouped = tupels.groupBy(_._10)
    }else if(index == 11){
      grouped = tupels.groupBy(_._11)
    }else if(index == 12){
      grouped = tupels.groupBy(_._12)
    }else if(index == 13){
      grouped = tupels.groupBy(_._13)
    }else {
      println("Error")
    }

    //val grouped: RDD[(String, Iterable[(Int, Int, Int, Int, Int, Int, Int, Double, Double, String, String, String, String)])] = tupels.groupBy(x=>{index})
    val summed: RDD[(String, Int, Int, Int, Int, Int, Int, Int, Double, Double)] = grouped.map(x=>{
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
        if(tuple._1 == 1 && tuple._2 >= 1){
          originalRequestNum += 1
        }
        if(tuple._1 == 1 && tuple._2 >= 2){
          effectiveRequestNum += 1
        }
        if(tuple._1 == 1 && tuple._2 == 3){
          adRequestNum += 1
        }
        if(tuple._3 == 1 && tuple._4 == 1 && tuple._5 == 1){
          joinBiddingNum += 1
          if(tuple._3 == 1 && tuple._4 == 1 && tuple._6 == 1 && tuple._7 != 0){
            complitedBiddingNum += 1
            dspConsume += tuple._8/1000
            dspCost += tuple._9/1000
          }
        }
        if(tuple._1 == 2 && tuple._3 == 1){
          displayNum += 1
        }
        if(tuple._1 == 3 && tuple._2 == 1){
          clickNum += 1
        }

      }
      (x._1,originalRequestNum, effectiveRequestNum, adRequestNum , joinBiddingNum , complitedBiddingNum , displayNum , clickNum, dspConsume,dspCost)
    })
    summed
  }



  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val Array(inputPath,outputPath) = args
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    /**
      * 方法二
      * */
    //val provincename = x.getAs("provincename").toString
    //      val cityname = x.getAs("cityname").toString
    val tupels: RDD[(Int, Int, Int, Int, Int, Int, Int, Double, Double, String, String, String, String)] =df.rdd.map(x=>{

      val requestmode = x.getAs("requestmode").toString.toInt
      val processnode = x.getAs("processnode").toString.toInt
      val iseffective = x.getAs("iseffective").toString.toInt
      val isbilling = x.getAs("isbilling").toString.toInt
      val isbid = x.getAs("isbid").toString.toInt
      val iswin = x.getAs("iswin").toString.toInt
      val adorderid = x.getAs("adorderid").toString.toInt
      val winprice = x.getAs("winprice").toString.toDouble
      val adpayment = x.getAs("adpayment").toString.toDouble
      //
      val ispname = x.getAs("ispname").toString
      //
      val networkmannername = x.getAs("networkmannername").toString
      //设备类
      val device = x.getAs("device").toString
      //操作系统
      val client = x.getAs("client").toString

      (requestmode,processnode,iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment,ispname,networkmannername,device,client)
    })
    //按运营商分组
    val ispSummed: RDD[(String, Int, Int, Int, Int, Int, Int, Int, Double, Double)] = groupByAndSum(tupels,10)
    //按网络类型分组
    val networkmannernameSummed: RDD[(String, Int, Int, Int, Int, Int, Int, Int, Double, Double)] = groupByAndSum(tupels,11)
    //按设备类型分组
    val deviceSummed = groupByAndSum(tupels,12)
    //按操作系统类型分组
    val clientSummed = groupByAndSum(tupels,13)
    ispSummed.foreach(println)
    networkmannernameSummed.foreach(println)
    deviceSummed.foreach(println)
    clientSummed.foreach(println)


    /**
      * DataFrame方式
      * */
    import sQLContext.implicits._
    val tup = df.map(row=>{
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
      //
      val ispname = row.getAs[String]("ispname")
      //
      val networkmannername = row.getAs[String]("networkmannername")
      //设备类
      val device = row.getAs[String]("device")
      //操作系统
      val client = row.getAs[Int]("client")
      val matchRequestList: List[Double] = DMPUtils.matchRequest(requestmode,processnode)
      val matchBiddingList: List[Double] = DMPUtils.matchBidding(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val matchAdList: List[Double] = DMPUtils.matchAd(requestmode,iseffective)
      (ispname
        ,matchRequestList ++ matchBiddingList ++ matchAdList)
    })

    val res = tup.rdd.reduceByKey((list1, list2)=>{
      val t: List[(Double, Double)] = list1.zip(list2)
       t.map(x=>x._1+x._2)
    })
    val result = res.map(x=>{
      (x._1,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8))})
    //result.foreach(println)
    result.saveAsTextFile(outputPath)
  }
}
