//package tags.user
//
//import com.dmp.beans.Logs
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by Administrator on 2018/5/17.
//  */
//object UserTags {
//  def main(args: Array[String]): Unit = {
//    //判断参数
////    if(args.length < 3){
////      println(
////        """
////          |com.dmp.tags.user.UserTags
////          |<inputTagsPath> 上下文合并的标签的结果
////          |<shipsPath> 用户关系表路径
////          |<resultTagsOutputPath> 最终结果输出
////        """.stripMargin)
////      System.exit(0)
////    }
//    //接收参数
//    //创建程序入口
//    val conf = new SparkConf()
//    conf.setMaster("local")
//    conf.setAppName("UserGrpah")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    conf.registerKryoClasses(Array(classOf[Logs]))
//    val sc = new SparkContext(conf)
//    // 6) 读取昨天上下文合并的标签数据
//    val ut = sc.textFile("D:\\1711班\\第二十三天\\资料\\上下文标签数据.txt")
//      .map(line => {
//        val fields = line.split("\t")
//        val tags = fields.slice(1, fields.length).flatMap(kv => {
//          var map = Map[String, Int]()
//          val tkv = kv.split(":")
//          map += (tkv(0) -> tkv(1).toInt)
//          map
//        })
//        (fields(0), tags.toList)
//      })
//    /**
//      *
//      * (李希沅,List((D00020005,2), (D00010003,2), (APP马上赚,2), (ZC上海市,2), (D00010001,2), (LC00,2), (ZP上海市,2)))
//        (美男子,List((D00020005,2), (D00010003,2), (APP马上赚,1), (ZC上海市,2), (D00010001,2), (LC00,2), (ZP上海市,2)))
//      *
//      */
//    //7) 读取用户关系表
//
//    val us = sc.textFile("D:\\1711班\\第二十三天\\资料\\ship.txt")
//      .flatMap(line => {
//        val fields = line.split("\t")
//        fields.slice(1, fields.length).map(t => (t, fields(0)))
//      })
//
//    // 8)
//    /**
//      * (userid,(hashCodeid,tags))
//      */
//    us.join(ut).map{
//      case(uId,(hashCode,tags)) => (hashCode,tags)
//    }.reduceByKey{
//      case(list1,list2) =>{
//        (list1 ++ list2).groupBy(_._1).map{
//          case(tk,stk) =>{
//            val sum = stk.map(_._2).sum
//            (tk,sum)
//          }
//        }.toList
//      }
//    }.foreach(println(_))
//
//  }
//
//}
