package cn.dmp.SparkAndHBase


import java.time.LocalDate

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD


object SparkWriteHbase {
  def writer(result: RDD[(String, List[(String, Int)])],tablename:String) = {
    val day = LocalDate.now()
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","192.168.40.82,192.168.40.83,192.168.40.84")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = connection.getAdmin
    // 如果表不存在则创建表
    //val admin = new HBaseAdmin(conf)
    if (!admin.tableExists(TableName.valueOf(tablename))) {

      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      // 创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加入表中
      tableDesc.addFamily(columnDescriptor)
      admin.createTable(tableDesc)
      admin.close()
      connection.close()
    }else{
      admin.disableTable(TableName.valueOf(tablename))
      admin.deleteTable(TableName.valueOf(tablename))
    }

    /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
    val out = result.map{
      case (userid,userTags) => {
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$day"), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }}
    out.saveAsHadoopDataset(jobConf)
  }
}
