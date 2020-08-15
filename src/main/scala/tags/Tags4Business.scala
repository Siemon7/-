package tags

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.{Jedis, JedisPool}
import utils.JedisPools


object Tags4Business extends Tags {
  /**
   * 打标签的方法
   * 可以传多个对象
   * 因为有些标签传入的个数不确定，例如有些标签还可能需要传入映射文件(缓存文件)，所以需要 args:Any*
   */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val jedis:Jedis = args(1).asInstanceOf[Jedis]
    val lat = row.getAs[String]("lat")
    val lon= row.getAs[String]("long1")

    if(StringUtils.isNotEmpty(lat) && StringUtils.isNotEmpty(lon)){
      //lat >= 3 and lat < 54 and lat != '' and lon >= 73 and lon <= 136
      val lat2 = lat.toDouble
      val lon2 = lon.toDouble
      if(lat2 >3 && lat2 < 54 && lon2 > 73 && lon2 < 136){
        val geoCode: String = GeoHash.withCharacterPrecision(lat2,lon2,8).toBase32
        val business: String = jedis.get(geoCode)
        if(StringUtils.isNotEmpty(business)){
          business.split(",").foreach(b => map += ("B" + b ->1))
        }
      }
    }
    map
  }
}
