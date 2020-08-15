package report

import java.util.Properties

import utils.{JedisPools, RptUtils}
import beans.Log
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 广告投放的地域分布统计
  * 实现方式：
  * Spark SQL
  */
object AreaAnalyseRpt2 {

    def main(args: Array[String]): Unit = {

        // 0 校验参数个数
        if (args.length != 2) {
            println(
                """
                  |cn.dmp.report.AreaAnalyseRpt
                  |参数：
                  | logInputPath
                  | outputPath
                """.stripMargin)
            sys.exit()
        }

        // 1 接受程序参数
        val Array(inputPath, outputPath) = args

        // 2 创建sparkconf->sparkContext
        val sc = SparkSession.builder()
          .appName(s"${this.getClass.getSimpleName}")
          .master("local[2]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        // 读取数据
      sc.sparkContext.textFile(inputPath)
        .map(_.split(",", -1))
        .filter(_.length >= 85)
        .map(arr => {
          val log = Log(arr)

          val req = RptUtils.caculateReq(log.requestmode, log.processnode)
          val rtb = RptUtils.caculateRtb(log.iseffective, log.isbilling,log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
          val showClick = RptUtils.caculateShowClick(log.requestmode, log.iseffective)

          ((log.provincename, log.cityname), req ++ rtb ++ showClick)
          // (省，地市，媒体，渠道，操作系统，网络类型,...，List(9个指标数据))
        }).reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      }).map(t => t._1._1+","+t._1._2+","+t._2.mkString(","))
        .saveAsTextFile(outputPath)



      // 读取parquet文件
      /*val sQLContext = new SQLContext(sc)
      val parquetData: DataFrame = sQLContext.read.parquet(logInputPath)
      parquetData.map(row =>{
          // 是不是原始请求，有效请求，广告请求 List(原始请求，有效请求，广告请求)
          val reqMode = row.getAs[Int]("requestmode")
          val prcNode = row.getAs[Int]("processnode")
          // 参与竞价, 竞价成功  List(参与竞价，竞价成功, 消费, 成本)
          val effTive = row.getAs[Int]("iseffective")
          val bill = row.getAs[Int]("isbilling")
          val bid = row.getAs[Int]("isbid")
          val orderId = row.getAs[Int]("adorderid")
          val win = row.getAs[Int]("iswin")
          val winPrice = row.getAs[Double]("winprice")
          val adPayMent = row.getAs[Double]("adpayment")


          val reqList = RptUtils.caculateReq(reqMode, prcNode)
          val rtbList = RptUtils.caculateRtb(effTive,bill,bid,orderId,win,winPrice,adPayMent)
          val showClickList = RptUtils.caculateShowClick(reqMode,effTive)

          // 返回元组
          ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), reqList++rtbList++showClickList)

      }).reduceByKey((list1, list2) => {
          list1.zip(list2).map(t => t._1 + t._2)
      }).map(t => t._1._1+","+t._1._2+","+t._2.mkString(","))
        .saveAsTextFile(resultOutputPath)*/

      sc.stop()

    }

}
