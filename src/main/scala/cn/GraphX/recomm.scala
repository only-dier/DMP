package cn.GraphX

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

object recomm {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("recomm").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val seqRDD = sc.makeRDD(Seq(
      (1L,("22群",22)),
      (2L,("22群",22)),
      (3L,("22群",22)),
      (22L,("22群",22)),
      (33L,("33群",22)),
      (32L,("33群",22)),
      (42L,("33群",22)),
      (23L,("33群",22)),
      (21L,("12群",22)),
      (654L,("12群",22)),
      (54L,("12群",22))
    ))
    val edgeRDD = sc.makeRDD(Seq(
      Edge(1L,22L,0),
      Edge(2L,22L,0),
      Edge(3L,22L,0),
      Edge(22L,22L,0),
      Edge(33L,33L,0),
      Edge(32L,33L,0),
      Edge(42L,33L,0),
      Edge(23L,33L,0),
      Edge(21L,12L,0),
      Edge(654L,12L,0),
      Edge(54L,12L,0)
    ))
    val graph: Graph[(String, Int), Int] = Graph(seqRDD,edgeRDD)
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    vertices.join(seqRDD).map{
      case (userid,(vid,(name,age))) =>(vid,List(name,age))
    }.reduceByKey(_++_).foreach(println)
    sc.stop()
  }
}
