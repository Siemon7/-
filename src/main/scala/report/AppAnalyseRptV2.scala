package report

import beans.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import utils.{JedisPools, RptUtils}

import scala.collection.mutable.ListBuffer

object AppAnalyseRptV2 {

    def main(args: Array[String]): Unit = {

        if (args.length != 2) {
            println(
                """
                  |cn.dmp.report.AppAnalyseRptV2
                  |参数：
                  | 输入路径
                  | 输出路径
                """.stripMargin)
            sys.exit()
        }

        val Array(inputPath, outputPath) = args


        // 2 创建sparkconf->sparkContext
        val sparkConf = new SparkConf()
        sparkConf.setAppName(s"${this.getClass.getSimpleName}")
        sparkConf.setMaster("local[*]")
        // RDD 序列化到磁盘 worker与worker之间的数据传输
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)


        // 读取数据
        sc.textFile(inputPath)
          .map(_.split(",", -1))
          .filter(_.length >= 85)
          .map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
          .mapPartitions(itr => {
              val jedis = JedisPools.getJedis()

//              val parResult = new collection.mutable.ListBuffer[(String, List[Double])]()
            val parResult = ListBuffer[(String, List[Double])]()

              // 遍历分区的所有数据，查询redis（把appname为空的数据进行转换），将结果放入到Listbuffer
              itr.foreach(log => {
                  var newAppName = log.appname
                  if (StringUtils.isEmpty(newAppName)) {
                      newAppName = jedis.get(log.appid)
                  }

                  val req = RptUtils.caculateReq(log.requestmode, log.processnode)
                  val rtb = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
                  val showClick = RptUtils.caculateShowClick(log.requestmode, log.iseffective)

                  parResult += ((newAppName, req ++ rtb ++ showClick))
              })

              jedis.close()
              // 将listbuffer转成迭代器
              parResult.toIterator
          }).reduceByKey((list1, list2) => {
            list1.zip(list2).map(t => t._1 + t._2)
        }).map(t => t._1 + "," + t._2.mkString(","))
          .saveAsTextFile(outputPath)


        sc.stop()

    }

}
