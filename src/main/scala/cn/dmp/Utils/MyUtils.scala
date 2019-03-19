package cn.dmp.Utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MyUtils {
  def saveToMysql(it: Iterator[(String, Int)]): Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //先获得一个JDBC连接
    val conn: Connection = DriverManager.getConnection(
      "jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8", "root", "654321")
    //将数据通过Connection写入到数据库
    val pstm: PreparedStatement = conn.prepareStatement(
      "INSERT INTO access_log_provinceCount VALUES (?, ?)")
    //将分区中的数据一条一条写入到MySQL中
    it.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    //将分区中的数据全部写完之后，在关闭连接
    if(pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}
