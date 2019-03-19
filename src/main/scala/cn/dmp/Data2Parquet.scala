package scala.cn.dmp


import java.util.Properties

import cn.dmp.Utils.DMPUtils
import cn.dmp.pojo.Log
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
object Data2Parquet {



  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("参数不对")
      sys.exit()
    }
    val conf: SparkConf = new SparkConf().setAppName("da").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.broadcast.compress","true")
    //此为spark io 配置conf.set("spark.io.compression.codec","org.apache.spark.io.SnappyCompressionCodec")

    //conf.set("spark.debug.maxToStringFields", "100")
    conf.registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)
    val sqLContext = new SQLContext(sc)
    //spark2.0以后 默认snappy
    //sqLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val files: RDD[String] = sc.textFile(args(0))
    val data: RDD[Array[String]] = files.map(x => x.split(",", x.length)).filter(y => y.length >= 85)

    val rowRDD: RDD[Row] = data.map(x => {

      val rowRDD: Row = DMPUtils.transToRow(x)
      rowRDD
    })
    val structType: StructType = DMPUtils.createStructType()
    val df: DataFrame = sqLContext.createDataFrame(rowRDD,structType)


    /**
      * 方法二
      * */
   val user = data.map(array => {

      val user = Log(
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
        DMPUtils.toInt(array(84)))
     user
    })
    import sqLContext.implicits._
    val df2 = user.toDF()
    //df2.show()


    df.createTempView("logTable")
    //df.show()
    //df.show(false)

    sqLContext.sql("select * from logTable").write.save(args(1))
    //sqLContext.sql("select provincename,cityname,count(*) as ct from logTable group by provincename,cityname partitioned by provincename").createTempView("table2")
    //sqLContext.sql("select provincename,cityname,t2.ct,rank() over(order by t2.ct desc) as rank from table2 t2").show()
    //sqLContext.sql("select provincename,cityname,t2.ct,dense_rank() over(order by t2.ct desc) as rank from table2 t2").show()
    //sqLContext.sql("select provincename,cityname,t2.ct,row_number() over(partition by provincename order by t2.ct desc) as rank from table2 t2").write.parquet(args(1))
    //.write.json(args(1))
    //在resource中，优先级：applicatio.conf > json > properties

    val load: Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("password",load.getString("jdbc.password"))

    sc.stop()
  }
}

