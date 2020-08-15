package tags

import beans.Log
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis}
import utils.{JedisPools, TagUserIDUtil}

import scala.collection.mutable.ListBuffer

/**
  * 7）上下文标签：将数据打上上述6类标签，并根据【用户ID】进行当前文件的合并，数据保存格式为
  *
  * 运行参数为：
  * G:\光环国际大数据开发班\大数据最后阶段-项目\22-dmp项目\资料\data.txt G:\光环国际大数据开发班\大数据最后阶段-项目\22-dmp项目\资料\appmapping.txt G:\光环国际大数据开发班\大数据最后阶段-项目\22-dmp项目\资料\device_mapping.txt xx
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //1.判断参数
    if (args.length != 4) {
      println(
        """
          |tags.TagsContext
          |<inputLogPath> 输入的日志文件路径
          |<appDicInputPath> app字典路径
          |<stopWordsDicInputPath>停用词汇
          |<outputPath> 输出的结果文件存储
        """.stripMargin)
      System.exit(0)
    }
    //2.接受参数
    val Array(logInputPath,appDicInputPath,stopWordsDicInputPath,resultOutputPath)  = args
    //3.创建长下文

    val sc = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    //读取app字典文件
    val appDic: Map[String, String] = sc.sparkContext.textFile(appDicInputPath).map(line => {
      val fields: Array[String] = line.split(":")
      (fields(0), fields(1))
    }).collect.toMap
    //将app字典广播出去
    val appDicBC: Broadcast[Map[String, String]] = sc.sparkContext.broadcast(appDic)

    //读取stopwords字典
    val stopwordsDic: Map[String, Int] = sc.sparkContext.textFile(stopWordsDicInputPath).map(line =>(line,1)).collect().toMap
    //将stopwords 字典广播出去
    val stopwordsDicBC: Broadcast[Map[String, Int]] = sc.sparkContext.broadcast(stopwordsDic)

    //4.读取parquet文件
    /**
     * 读取parquet文件数据
     */

    val rawDF: DataFrame = sc.read.parquet(logInputPath)
    //5.过滤出去用户唯一标识不存在的数据
    val filterdDS: Dataset[Row] = rawDF.where(TagUserIDUtil.hasOneUserID)
    import sc.implicits._
    //6.打标签
    val tagedRDD: RDD[(String, List[(String, Int)])] = filterdDS.mapPartitions(it => {
      val jedis: Jedis = JedisPools.getJedis
      var list = new ListBuffer[(String, List[(String, Int)])]()
      it.foreach(row => {
        //打广告的标签
        val tagsAds: Map[String, Int] = Tags4Ads.makeTags(row,jedis)
        //app标签
        val tagsApp: Map[String, Int] = Tags4App.makeTags(row, appDicBC.value)
        //设备标签
        val tagsDevice: Map[String, Int] = Tags4Device.makeTags(row)
        //关键词标签
        val tagKeyWords: Map[String, Int] = Tags4KeyWords.makeTags(row, stopwordsDicBC.value)
        //地域标签
        val tagArea: Map[String, Int] = Tags4Area.makeTags(row)
        //商圈标签
        val tagBusiness: Map[String, Int] = Tags4Business.makeTags(row, jedis)
        //获取用户的唯一标识
        val userID= TagUserIDUtil.getAnyOneUserID(row)
        list.append()
        val tuple: (String, List[(String, Int)]) = (userID, (tagsAds ++ tagsApp ++ tagsDevice ++ tagKeyWords ++ tagArea ++ tagBusiness).toList)
        list.append(tuple)
      })
      jedis.close()
      list.iterator
    }).rdd
    //聚合
    val reduceRDD: RDD[(String, List[(String, Int)])] = tagedRDD.reduceByKey((a, b) => {
      //List(("K偶像剧",1),("K偶像剧",1),("ZP河北"，1))
      //方式一
      (a ++ b).groupBy(_._1).mapValues(_.length).toList
      //方式二
      //(a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
      //方式三
//      (a ++ b).groupBy(_._1).map{
//        case (k,v) => (k,v.foldLeft(0)(_+_._2))
//      }.toList
    })
    //将数据写入到磁盘
//    OutputPathUtil.deleteOutputPath(resultOutputPath,sc)

    reduceRDD.saveAsTextFile(resultOutputPath)
    //关闭SparkSession
    sc.stop()
  }

}

//    val lineDF: DataFrame = sc.read.parquet(logInputPath)
//    import sc.implicits._
//  lineDF.filter(TagUserIDUtil.hasOneUserID)
//      .mapPartitions(iter => {
//        val jedis = JedisPools.getJedis()
//
//        val resIter = iter.map(row => {
//          //广告相关标签
//          val ad: List[(String, Int)] = Tags4Ads.makeTags(row, jedis)
//          //设备相关标签
//          val device: List[(String, Int)] = Tags4Device.makeTags(row).toList
//          //关键字相关标签
//          val keywords: List[(String, Int)] = Tags4KeyWords.makeTags(row, jedis).toList
//          //地域相关标签
//          val area: List[(String, Int)] = Tags4Area.makeTags(row).toList
//          //商圈相关标签
//          val business: List[(String, Int)] = Tags4Business.makeTags(row, jedis).toList
//
//          //获取该行上的用户id
//          val userId = TagUserIDUtil.getAnyOneUserID(row)
//          //(userId,List((k,1)))
//          (userId, ad ++ device ++ keywords ++ area ++ business)
//        })
//
//        jedis.close()
//        resIter
//      }).rdd
//
//    value.reduce
//
//    ((list1, list2) => {
//      (list1 ++ list2).groupBy(_._1).mapValues(_.length).toList
//    }).saveAsTextFile("E:\\projectdata\\dmp\\tags2")
//
//    sc.stop()
//  }
//
//}


