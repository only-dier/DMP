package cn.dmp.Tag

trait Tags {
  //打标签接口
  def label(args:Any*): List[(String, Int)]

}
