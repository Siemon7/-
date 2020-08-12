package report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 广告投放的地域分布统计
  * 实现方式：
  * Spark SQL
  */
object AreaAnalyseRpt2 {

    def main(args: Array[String]): Unit = {

        // 0 校验参数个数
        if (args.length != 1) {
            println(
                """
                  |cn.dmp.report.AreaAnalyseRpt
                  |参数：
                  | logInputPath
                """.stripMargin)
            sys.exit()
        }

        // 1 接受程序参数
        val Array(logInputPath) = args

        // 2 创建sparkconf->sparkContext
        val sc = SparkSession.builder()
          .appName(s"${this.getClass.getSimpleName}")
          .master("local[2]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        //读取parquet文件
        val parquetData = sc.read.parquet(logInputPath)


        // dataframe -> table
        parquetData.registerTempTable("log")

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

            if(reqMode == 1 && prcNode == 1){
                List[Double](1,0,0)
            }else if (reqMode == 1 && prcNode == 2){
                List[Double](1,1,0)
            }else if(reqMode == 1 && prcNode == 3){
                List[Double](1,1,1)
            }

            if (effTive == 1 && bill == 1 && bid == 1 && orderId != 0) {
                List[Double](1, 0, 0, 0)
            } else if (effTive == 1 && bill == 1 && win == 1) {
                List[Double](0, 1, winPrice / 1000.0, adPayMent / 1000.0)
            } else List[Double](0, 0, 0, 0)

            if (reqMode == 2 && effTive == 1) {
                List[Double](1, 0)
            } else if (reqMode == 3 && effTive == 1) {
                List[Double](0, 1)
            } else List[Double](0, 0)


        }









            // 加载配置文件  application.conf -> application.json --> application.properties
        val load = ConfigFactory.load()
        val props = new Properties()
        props.setProperty("user", load.getString("jdbc.user"))
        props.setProperty("password", load.getString("jdbc.password"))
        result.show()

        result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), load.getString("jdbc.arearpt.table"), props)


        sc.stop()
    }

}

