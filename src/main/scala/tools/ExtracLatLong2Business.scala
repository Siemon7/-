package tools

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import utils.{BaiDuGeoApi, JedisPools}


object ExtracLatLong2Business {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        """
          |tool.ExtracLatLong2Business
          |参数错误！！！
          |需要:inputPath
        """.stripMargin)
      sys.exit()
    }

    // 1 接受程序参数
    val Array(inputPath) = args

    // 2 创建sparkconf->sparkContext

    val sc = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    sc.read.parquet(inputPath)
      .select("lat", "long1")
      .where("lat >= 3 and lat < 54 and lat != '' and long1 >= 73 and long1 <= 136 and long1 != ''")
      .distinct()
      .foreachPartition(it => {
        val jedis: Jedis = JedisPools.getJedis
        it.foreach(row => {
          val lat: String = row.getAs[String]("lat")
          val lon: String = row.getAs[String]("long1")
//          println(lat + "," + lon)
          //根据lat lon 去百度获取出商圈信息 "火焰山美食城,点点超市，花冠超市"
          val business: String = BaiDuGeoApi.getBusiness(lat + "," + lon)
          //使用GeoHash算法根据lat lon 获取GeoHashCode 作为可以
          val geocode: String = GeoHash.withCharacterPrecision(lat.toDouble, lon.toDouble, 8).toBase32
          if (StringUtils.isNotEmpty(business)) {
            jedis.set(geocode, business)
          }
        })
        jedis.close()

      })
    //关闭SparkSession
    sc.stop()
  }
}




