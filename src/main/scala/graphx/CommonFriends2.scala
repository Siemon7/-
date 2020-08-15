package graphx


import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 案例数据：
  *   张三 Angelababy 波多
  *   波多 老赵
  *   老段 老赵 老羊
  *   刘亦菲
  *
  * Author: jrx
  * Created: 2018/7/25 
  */
object CommonFriends2 {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取数据
    val data = sc.sparkContext.textFile("E:\\projectdata\\dmp\\Graph_03_data.txt")
      .map(_.split("\t"))

    //点集合 RDD[(Long,VD)]
    val verticesRDD: RDD[(VertexId, String)] = data.flatMap(currentLineNames => {
      currentLineNames.map(name => (name.hashCode.toLong, name))
    })

    //边集合 RDD[Edge<ED>]
    val edgesRDD: RDD[Edge[Int]] = data.flatMap(currentLineNames => {
      currentLineNames.map(name => Edge(currentLineNames.head.hashCode.toLong, name.hashCode.toLong, 0))
    })

    //图对象
    val graph: Graph[String, Int] = Graph.apply(verticesRDD,edgesRDD)
    //connected components
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //(20854308,Set(刘亦菲))
    //(774889,Set(波多, 老羊, Angelababy, 老赵, 老段, 张三))
    cc.join(verticesRDD).map{
      case (id,(minId,name)) => (minId,Set(name))
    }.reduceByKey(_++_).foreach(println)

    sc.stop()
  }

}
