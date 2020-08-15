package report

import java.util.Properties

import beans.Log
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计日志文件中省市的数据量分布情况
  *
  *  本次统计是基于parquet文件
  *
  * 需求1：
  *     将统计出来的结果存储成json文件格式
  *
  * 需求2：
  *     将统计出来的结果存储到mysql中
  */
object ProCityRpt2 {

    def main(args: Array[String]): Unit = {

        // 0 校验参数个数
        if (args.length != 1) {
            println(
                """
                  |cn.dmp.report.ProCityRpt2
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

        // 将dataframe注册成一张临时表
        val df: DataFrame = sc.read.parquet(logInputPath)
        df.createOrReplaceTempView("log")

        // 按照省市进行分组聚合---》统计分组后的各省市的日志记录条数
        val result: DataFrame = sc.sql("select * from log ")

        //         判断结果存储路径是否存在，如果存在则删除
//        val hadoopConfiguration = sc.sparkContext.hadoopConfiguration
//        val fs = FileSystem.get(hadoopConfiguration)
//        val resultPath = new Path(resultOutputPath)
//        if(fs.exists(resultPath)) {
//            fs.delete(resultPath, true)
//        }


        // 加载配置文件  application.conf -> application.json --> application.properties
        val load = ConfigFactory.load()
        val props = new Properties()
        props.setProperty("user", load.getString("jdbc.user"))
        props.setProperty("password", load.getString("jdbc.password"))

        // 将结果写入到mysql的 rpt_pc_count 表中
        result.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), load.getString("jdbc.tableName"), props)


        sc.stop()
    }

}
