package cn.dmp.Utils

import cn.dmp.Utils.Jedis.JedisConnectionPool

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis
object DMPUtils {
  def addAppToRedis(map: Map[String, String]) = {
    val jedis: Jedis = JedisConnectionPool.getConnection()
    map.foreach(x=>{
      jedis.set(x._1,x._2)
    })
    jedis.close()
  }

  def createStructType(): StructType = {
    val structType = StructType(Array(
      StructField("sessionid",StringType,true),
      StructField("advertisersid",IntegerType,true),
      StructField("adorderid",IntegerType,true),
      StructField("adcreativeid",IntegerType,true),
      StructField("adplatformproviderid",IntegerType,true),
      StructField("sdkversion",StringType,true),
      StructField("adplatformkey",StringType,true),
      StructField("putinmodeltype",IntegerType,true),
      StructField("requestmode",IntegerType,true),
      StructField("adprice",DoubleType,true),
      StructField("adppprice",DoubleType,true),
      StructField("requestdate",StringType,true),
      StructField("ip",StringType,true),
      StructField("appid",StringType,true),
      StructField("appname",StringType,true),
      StructField("uuid",StringType,true),
      StructField("device",StringType,true),
      StructField("client",IntegerType,true),
      StructField("osversion",StringType,true),
      StructField("density",StringType,true),
      StructField("pw",IntegerType,true),
      StructField("ph",IntegerType,true),
      StructField("long",StringType,true),
      StructField("lat",StringType,true),
      StructField("provincename",StringType,true),
      StructField("cityname",StringType,true),
      StructField("ispid",IntegerType,true),
      StructField("ispname",StringType,true),
      StructField("networkmannerid",IntegerType,true),
      StructField("networkmannername",StringType,true),
      StructField("iseffective",IntegerType,true),
      StructField("isbilling",IntegerType,true),
      StructField("adspacetype",IntegerType,true),
      StructField("adspacetypename",StringType,true),
      StructField("devicetype",IntegerType,true),
      StructField("processnode",IntegerType,true),
      StructField("apptype",IntegerType,true),
      StructField("district",StringType,true),
      StructField("paymode",IntegerType,true),
      StructField("isbid",IntegerType,true),
      StructField("bidprice",DoubleType,true),
      StructField("winprice",DoubleType,true),
      StructField("iswin",IntegerType,true),
      StructField("cur",StringType,true),
      StructField("rate",DoubleType,true),
      StructField("cnywinprice",DoubleType,true),
      StructField("imei",StringType,true),
      StructField("mac",StringType,true),
      StructField("idfa",StringType,true),
      StructField("openudid",StringType,true),
      StructField("androidid",StringType,true),
      StructField("rtbprovince",StringType,true),
      StructField("rtbcity",StringType,true),
      StructField("rtbdistrict",StringType,true),
      StructField("rtbstreet",StringType,true),
      StructField("storeurl",StringType,true),
      StructField("realip",StringType,true),
      StructField("isqualityapp",IntegerType,true),
      StructField("bidfloor",DoubleType,true),
      StructField("aw",IntegerType,true),
      StructField("ah",IntegerType,true),
      StructField("imeimd5",StringType,true),
      StructField("macmd5",StringType,true),
      StructField("idfamd5",StringType,true),
      StructField("openudidmd5",StringType,true),
      StructField("androididmd5",StringType,true),
      StructField("imeisha1",StringType,true),
      StructField("macsha1",StringType,true),
      StructField("idfasha1",StringType,true),
      StructField("openudidsha1",StringType,true),
      StructField("androididsha1",StringType,true),
      StructField("uuidunknow",StringType,true),
      StructField("userid",StringType,true),
      StructField("iptype",IntegerType,true),
      StructField("initbidprice",DoubleType,true),
      StructField("adpayment",DoubleType,true),
      StructField("agentrate",DoubleType,true),
      StructField("lomarkrate",DoubleType,true),
      StructField("adxrate",DoubleType,true),
      StructField("title",StringType,true),
      StructField("keywords",StringType,true),
      StructField("tagid",StringType,true),
      StructField("callbackdate",StringType,true),
      StructField("channelid",StringType,true),
      StructField("mediatype",IntegerType,true)
    ))
    structType
  }

  def toInt(str: String): Int = {
    try{
      str.toInt
    }catch {
      case _ : NumberFormatException => 0
    }
  }
  def toDouble(str:String):Double = {
    try{
      str.toDouble
    }catch {
      case _ : NumberFormatException => 0.0
    }
  }
  def transToRow(array: Array[String]): Row = {
    Row(
      array(0),
      DMPUtils.toInt(array(1)),
      DMPUtils.toInt(array(2)),
      DMPUtils.toInt(array(3)),
      DMPUtils.toInt(array(4)),
      array(5),
      array(6),
      DMPUtils.toInt(array(7)),
      DMPUtils.toInt(array(8)),
      DMPUtils.toDouble(array(9)),
      DMPUtils.toDouble(array(10)),
      array(11),
      array(12),
      array(13),
      array(14),
      array(15),
      array(16),
      DMPUtils.toInt(array(17)),
      array(18),
      array(19),
      DMPUtils.toInt(array(20)),
      DMPUtils.toInt(array(21)),
      array(22),
      array(23),
      array(24),
      array(25),
      DMPUtils.toInt(array(26)),
      array(27),
      DMPUtils.toInt(array(28)),
      array(29),
      DMPUtils.toInt(array(30)),
      DMPUtils.toInt(array(31)),
      DMPUtils.toInt(array(32)),
      array(33),
      DMPUtils.toInt(array(34)),
      DMPUtils.toInt(array(35)),
      DMPUtils.toInt(array(36)),
      array(37),
      DMPUtils.toInt(array(38)),
      DMPUtils.toInt(array(39)),
      DMPUtils.toDouble(array(40)),
      DMPUtils.toDouble(array(41)),
      DMPUtils.toInt(array(42)),
      array(43),
      DMPUtils.toDouble(array(44)),
      DMPUtils.toDouble(array(45)),
      array(46),
      array(47),
      array(48),
      array(49),
      array(50),
      array(51),
      array(52),
      array(53),
      array(54),
      array(55),
      array(56),
      DMPUtils.toInt(array(57)),
      DMPUtils.toDouble(array(58)),
      DMPUtils.toInt(array(59)),
      DMPUtils.toInt(array(60)),
      array(61),
      array(62),
      array(63),
      array(64),
      array(65),
      array(66),
      array(67),
      array(68),
      array(69),
      array(70),
      array(71),
      array(72),
      DMPUtils.toInt(array(73)),
      DMPUtils.toDouble(array(74)),
      DMPUtils.toDouble(array(75)),
      DMPUtils.toDouble(array(76)),
      DMPUtils.toDouble(array(77)),
      DMPUtils.toDouble(array(78)),
      array(79),
      array(80),
      array(81),
      array(82),
      array(83),
      DMPUtils.toInt(array(84))
    )
  }
  def matchRequest(requestmode: Int, processnode: Int):List[Double] = {
    if(requestmode == 1 && processnode == 1){
      //发来的所有原始请求数 筛选满足有效条件的请求数量 筛选满足广告请求条件的请求数量
      List(1,0,0)
    }else if(requestmode == 1 && processnode == 2){
      List(1,1,0)
    }else if(requestmode == 1 && processnode == 3){
      List(1,1,1)
    }else{
      List(0,0,0)
    }
  }

  def matchBidding(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int, adorderid: Int,winprice: Double,adpayment :Double):List[Double] = {
    //参与竞价
    if(iseffective == 1 && isbilling==1 && isbid == 1){
      //赢得竞价
      if(iswin == 1 && adorderid != 0){
        //参与竞价的次数 成功竞价的次数 相对于投放DSP广告的广告主来说满足广告成功展示每次消费WinPrice/1000 相对于投放DSP广告的广告主来说满足广告成功展示每次成本adpayment/1000
        List(1,1,winprice/1000,adpayment/1000)
      }else{
        //未赢得竞价，只参与了竞价
        List(1,0,0.0,0.0)
      }
    }else{
      //未参与竞价
      List(0,0,0.0,0.0)
    }
  }

  def matchAd(requestmode: Int, iseffective: Int):List[Double] = {
    if(requestmode == 2 && iseffective == 1){
      //广告在终端实际被展示的数量 广告展示后被受众实际点击的数量
      List(1,0)
    }else if(requestmode == 3 && iseffective == 1){
      List(0,1)
    }else{
      List(0,0)
    }
  }
  def addAppToRedis() ={

  }

}
