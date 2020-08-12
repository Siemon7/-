package report

import java.util.Properties

import beans.Log
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广告投放的地域分布统计
  * 实现方式：
  * Spark SQL
  */
object AreaAnalyseRpt {

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

        // 业务逻辑
        val result = sc.sql(
            """
              |select
              |provincename, cityname,
              |sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) 有效请求,
              |sum(case when requestmode=1 and processnode =3 then 1 else 0 end) 广告请求,
              |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end) 参与竞价数,
              |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) 竞价成功数,
              |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) 展示数,
              |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) 点击数,
              |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*adpayment/1000 else 0 end) 广告成本,
              |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*winprice/1000 else 0 end) 广告消费
              |from log
              |group by provincename, cityname
            """.stripMargin)


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

