package cn.dmp

import cn.dmp.pojo.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Random

object localToLengAndLat {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("da").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.broadcast.compress","true")
    //此为spark io 配置conf.set("spark.io.compression.codec","org.apache.spark.io.SnappyCompressionCodec")

    //conf.set("spark.debug.maxToStringFields", "100")
    conf.registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)
    val sqLContext = new SQLContext(sc)


   // val Array(inputPath,outputPath) = args
    val files: RDD[String] = sc.textFile("D:\\Test\\dmp\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH.bz2")

    val areaFile = sc.textFile("D:\\Test\\dmp\\a.txt")
    val cityTolongAnLat = areaFile.map(x=>{
      val strings: Array[String] = x.split(",")
      val province: String = strings(0)
      val city: String = strings(1)
      val leng: String = strings(2)
      val lat: String = strings(3)
      //println("========="+province+city)
      (province+city+"市",leng+"_"+lat)
    }).collect().toMap
    val cityToMap: Broadcast[Map[String, String]] = sc.broadcast(cityTolongAnLat)

    val data: RDD[Array[String]] = files.map(x => x.split(",", x.length)).filter(y => y.length >= 85)
    val replace = data.map(x=>{
      var leng = x(22)
      var lat = x(23)
      val prov = x(24)
      val city = x(25)
      if(StringUtils.isBlank(leng)||StringUtils.isBlank(lat)){

          val result: String = cityToMap.value.getOrElse(prov+city,"1_1")
          val strings: Array[String] = result.split("_")
          leng = (strings(0).toDouble + Random.nextInt(100).toDouble/100).toString
          lat = (strings(1).toDouble + Random.nextInt(100).toDouble/100).toString
        }
        //113.65,34.76
        val str = x(0)+","+
        x(1)+","+
        x(2)+","+
        x(3)+","+
        x(4)+","+
        x(5)+","+
        x(6)+","+
        x(7)+","+
        x(8)+","+
        x(9)+","+
        x(10)+","+
        x(11)+","+
        x(12)+","+
        x(13)+","+
        x(14)+","+
        x(15)+","+
        x(16)+","+
        x(17)+","+
        x(18)+","+
        x(19)+","+
        x(20)+","+
        x(21)+","+
         leng+","+
          lat+","+
        x(24)+","+
        x(25)+","+
        x(26)+","+
        x(27)+","+
        x(28)+","+
        x(29)+","+
        x(30)+","+
        x(31)+","+
        x(32)+","+
        x(33)+","+
        x(34)+","+
        x(35)+","+
        x(36)+","+
        x(37)+","+
        x(38)+","+
        x(39)+","+
        x(40)+","+
        x(41)+","+
        x(42)+","+
        x(43)+","+
        x(44)+","+
        x(45)+","+
        x(46)+","+
        x(47)+","+
        x(48)+","+
        x(49)+","+
        x(50)+","+
        x(51)+","+
        x(52)+","+
        x(53)+","+
        x(54)+","+
        x(55)+","+
        x(56)+","+
        x(57)+","+
        x(58)+","+
        x(59)+","+
        x(60)+","+
        x(61)+","+
        x(62)+","+
        x(63)+","+
        x(64)+","+
        x(65)+","+
        x(66)+","+
        x(67)+","+
        x(68)+","+
        x(69)+","+
        x(70)+","+
        x(71)+","+
        x(72)+","+
        x(73)+","+
        x(74)+","+
        x(75)+","+
        x(76)+","+
        x(77)+","+
        x(78)+","+
        x(79)+","+
        x(80)+","+
        x(81)+","+
        x(82)+","+
        x(83)+","+
        x(84)
      str
    })
    replace.coalesce(1).saveAsTextFile("D:\\Test\\dmp\\out")
  }
}
