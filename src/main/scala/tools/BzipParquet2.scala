package tools

import beans.{Log, LogSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}



object BzipParquet2 {
  def main(args: Array[String]): Unit = {
    //校验程序的参数
    if(args.length != 2) {
      println(
        """
          |tools.BzipParquet2
          |参数：
          |logInputPath
          |resultOutputPath
          |""".stripMargin
      )
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath,resultOutputPath) = args

    // 2 创建sparkconf->sparkContext

    val sc = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val dataLog = sc.sparkContext.textFile(logInputPath)
      .map(line => line.split(",", -1))
      .filter(_.length >= 85)
      .map(arr => Log(arr))
    //读取日志数据


    //将结果存储到本地磁盘
    val dataFrame = sc.createDataFrame(dataLog)
    dataFrame.write.parquet(resultOutputPath)

    //关闭sc
    dataFrame.show()
    sc.stop

  }
}
