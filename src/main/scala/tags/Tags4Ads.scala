package tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  *
  * Author: jrx
  * Created: 2018/7/24 
  */
object Tags4Ads extends Tags {
  /**
    * 广告相关标签
    *
    * @param args 0:Row 1:Jedis
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]

    //广告位类型
    val adspacetype = row.getAs[Int]("adspacetype")
    if (adspacetype >= 10) list :+= ("LC" + adspacetype, 1) else if (adspacetype >= 0) list :+= ("LC0" + adspacetype, 1)
    val adspacetypename = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotEmpty("adspacetypename")) list :+= ("LN" + adspacetypename, 1)

    //App 名称
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if (StringUtils.isEmpty(appname)) {
      if (StringUtils.isNotEmpty("appid")) {
        val str: String = jedis.hget("appdict", appid)
        if (StringUtils.isNotEmpty(str)) list :+= ("APP" + str, 1) else list :+= ("APP" + appid, 1)
        val map = list.toMap
      }
    } else {
      list :+= ("APP" + appname, 1)


    }

    //渠道
    val channel = row.getAs[Int]("adplatformproviderid")
    if (channel > 0) list :+= ("CN" + channel, 1)


    return list.toMap
  }
}
