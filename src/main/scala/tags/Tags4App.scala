package tags

/**
  * 2）APP名称（标签格式：APPxxxx->1）xxxx为APP的名称，使用缓存文件appname_dict进行名称转换；
  */
import beans.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object Tags4App extends  Tags{
  /**
    * 打标签的方法
    * 给APP打标签
    * @param args
    *   args0:Logs
    *   args1:Map[String,String]:
    *           key:appID
    *           value:appName
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 1){
      val row = args(0).asInstanceOf[Row]
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      val appDict= args(1).asInstanceOf[Map[String,String]]
      val appName = appDict.getOrElse(appid,appname)
      if(StringUtils.isNotEmpty(appName) && !"".equals(appName)){
         map += ("APP"+appName -> 1)
      }
    }
    map
  }
}
