//package tags.user
//
//import com.dmp.beans.Logs
//import org.apache.spark.graphx.GraphLoader
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by Administrator on 2018/5/17.
//  * 生成用户关系表
//  */
//object UserGrpah {
//  def main(args: Array[String]): Unit = {
//    //判断参数
////    if(args.length < 3){
////       println(
////         """
////           |com.dmp.tags.user.UserGrpah
////           |<inputDataPath> 输入的日志路径
////           |<followersoutputPath> followers文件存储目录
////           |<usershipPath> 用户关系表存储目录
////         """.stripMargin)
////      System.exit(0)
////    }
////    //接收参数
////    val Array(intputDataPath,followeroutputPath,usershipPath)=args
//    //创建程序入口
//    val conf = new SparkConf()
//    conf.setMaster("local")
//    conf.setAppName("UserGrpah")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    conf.registerKryoClasses(Array(classOf[Logs]))
//    val sc = new SparkContext(conf)
//
//    //获取每条数据里面所有的ID号
////    val users: RDD[(Int, Set[String])] = sc.textFile(intputDataPath).map(line => {
////      val log = Logs.line2Log(line)
////      (Math.abs(getUserIds(log).hashCode()), getUserIds(log))
////    })
//    // 1）
//    val array = Array(
//      Tuple2(1L, Set("帅哥", "李希沅")),
//      Tuple2(2L, Set("美男子", "李希沅")),
//      Tuple2(3L, Set("李希沅"))
//    )
//    val users = sc.parallelize(array)
//    // 2)
//    users.flatMap( tuple =>{
//      tuple._2.map( id => (id,tuple._1.toString) )
//    })
//    /**
//      * (帅哥,1)
//        (李希沅,1)
//       (美男子,2)
//      (李希沅,2)
//       (李希沅,3)
//      */
//    // 3)
//      .reduceByKey((a:String,b:String)  => a.concat(",").concat(b))
//    /**
//      * (李希沅,1,2,3)
//       (美男子,2)
//      (帅哥,1)
//      */
//    //4)
//      .flatMap( t =>{
//      val ids = t._2.split(",")
//      var ships=new ArrayBuffer[(String,String)]()
//      if(ids.size == 1){
//        ships += Tuple2(ids(0),ids(0))
//      }else{
//        ids.map( id => ships += Tuple2(ids(0),id))
//      }
//      ships
//    }).foreach(println(_))
//    /**
//      * (1,1)
//       (1,2)
//       (1,3)
//       (2,2)
//       (1,1)
//      这个结果应该存储到followersoutputPath 目录下
//      存储的时候应该要注意以下，key和value之间用空格分开
//      */
//
//    //5 生成用户关系文件
//
//     val graph = GraphLoader.edgeListFile(sc,"D:\\1711班\\第二十三天\\文档\\myfollower.txt")
//
//     val cc = graph.connectedComponents().vertices
//
//    /**
//      * (1L,(names,lastid))
//      */
//    users.join(cc).map{
//      case(id,(names,minID)) =>{
//        (minID,names)
//      }
//    }.reduceByKey(_++_)
//      .foreach(tuple => println(Math.abs(tuple._2.hashCode()) + "->"+ tuple._2.mkString(",")))
//    /**
//      *
//      *1328503827->帅哥,李希沅,美男子
//      *
//      */
//
//
//    sc.stop()
//  }
//
//  /**
//    * 获取所有的userid
//    * @param log 传进来的日志对象
//    * @return 每条数据里面所有的id号
//    */
//  def getUserIds(log:Logs):Set[String]={
//    var ids=Set[String]()
//    if(log.imei.nonEmpty) ids++=Set("imei"+log.imei.toUpperCase())
//    if(log.imeimd5.nonEmpty) ids++=Set("IMEIMD5"+log.imeimd5.toUpperCase())
//    if(log.imeisha1.nonEmpty) ids ++=Set("IMEISHA1"+log.imeisha1.toUpperCase())
//
//    if (log.androidid.nonEmpty) ids ++= Set("ANDROIDID:"+log.androidid.toUpperCase)
//    if (log.androididmd5.nonEmpty) ids ++= Set("ANDROIDIDMD5:"+log.androididmd5.toUpperCase)
//    if (log.androididsha1.nonEmpty) ids ++= Set("ANDROIDIDSHA1:"+log.androididsha1.toUpperCase)
//
//    if (log.idfa.nonEmpty) ids ++= Set("IDFA:"+log.idfa.toUpperCase)
//    if (log.idfamd5.nonEmpty) ids ++= Set("IDFAMD5:"+log.idfamd5.toUpperCase)
//    if (log.idfasha1.nonEmpty) ids ++= Set("IDFASHA1:"+log.idfasha1.toUpperCase)
//
//    if (log.mac.nonEmpty) ids ++= Set("MAC:"+log.mac.toUpperCase)
//    if (log.macmd5.nonEmpty) ids ++= Set("MACMD5:"+log.macmd5.toUpperCase)
//    if (log.macsha1.nonEmpty) ids ++= Set("MACSHA1:"+log.macsha1.toUpperCase)
//
//    if (log.openudid.nonEmpty) ids ++= Set("OPENUDID:"+log.openudid.toUpperCase)
//    if (log.openudidmd5.nonEmpty) ids ++= Set("OPENUDIDMD5:"+log.openudidmd5.toUpperCase)
//    if (log.openudidsha1.nonEmpty) ids ++= Set("OPENUDIDSHA1:"+log.openudidsha1.toUpperCase)
//    ids
//  }
//
//
//
//
//}
