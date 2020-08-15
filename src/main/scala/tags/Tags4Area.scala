package tags

import beans.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 6)地域标签（省标签格式：ZPxxx->1，地市标签格式：ZCxxx->1）xxx为省或市名称
  */
object Tags4Area extends Tags{
  /**
    * 打标签的方法
    * 区域标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] ={
    var map=Map[String,Int]()
    if(args.length > 0){
      val row = args(0).asInstanceOf[Row]
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")

      //provincename 设备所在省份名称
      if(StringUtils.isNotEmpty(provincename)){
        map += ("ZP"+provincename -> 1)
      }
      //设备所在城市名称
      if(StringUtils.isNotEmpty(cityname)){
        map += ("ZC"+cityname -> 1)
      }
    }
    map
  }
}
