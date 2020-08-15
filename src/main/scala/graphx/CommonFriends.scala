package graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Graphx求共同好友
  * Author: jrx
  * Created: 2018/7/25 
  */
object CommonFriends {

  def main(args: Array[String]): Unit = {

    //  创建sparkconf->sparkContext
    val sc = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //顶点集合 RDD[(Long,<VD>)]
    val verticesRDD: RDD[(VertexId, String)] = sc.sparkContext.makeRDD(List(
      (1L, "jiang1"),
      (9L, "jiang9"),
      (2L, "jiang2"),
      (133L, "jiang133"),

      (6L, "jiang6"),

      (16L, "jiang16"),
      (21L, "jiang21"),
      (138L, "jiang138"),
      (44L, "jiang44"),

      (5L, "jiang5"),
      (7L, "jiang7"),
      (158L, "jiang158")
    ))

    //边集合 RDD[Edge<ED>]
    val edgesRDD: RDD[Edge[Int]] = sc.sparkContext.makeRDD(Seq(
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(9, 133, 0),
      Edge(6, 133, 0),
      Edge(6, 138, 0),
      Edge(16, 138, 0),
      Edge(21, 138, 0),
      Edge(44, 138, 0),
      Edge(7, 158, 0),
      Edge(5, 158, 0)
    ))
    //Graph对象
    val graph: Graph[String, Int] = Graph.apply(verticesRDD,edgesRDD)

    /**
      * connectedComponents（求点与点之间的关系） 可以找到图中可以联通的分支 -> 2个分支
      * vertices 每个联通图中的所有的点都会和该图分支中最小的点进行组合
      */
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //(5,Set(5, 158, 7))
    //(1,Set(9, 44, 16, 21, 138, 6, 1, 133, 2))
    cc.map(tp=>(tp._2,Set(tp._1.toString))).reduceByKey(_++_).foreach(println)

    //(1,Set(138, jiang2, jiang133, 133, 1, 6, jiang16, 21, jiang1, 9, 2, jiang6, 44, jiang21, jiang44, 16, jiang138, jiang9))
    //(5,Set(5, jiang158, jiang5, jiang7, 7, 158))
    cc.join(verticesRDD).map{
      case (id,(minId,name)) => (minId,Set(id,name))
    }.reduceByKey(_++_).foreach(println)

    sc.stop()
  }

}
