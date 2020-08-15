package tags

import beans.Log

/**
  * 4）设备：操作系统|联网方式|运营商
  */
object Tags4Device extends  Tags{
  /**
    * 打标签的方法
    * 设备标签：
    * 1）设备操作系统
    * 2）设备联网方式标签
    * 3）设备运营商方案标签
    * @param args
    *          args0:Logs
    *          args1:Map[String,String]
    *          key:WIFI
    *          value: D00020001
    * @return
    *
    * //注意在Map中.get("4")获取到的值是Option类型，需要再次.get()拿到里面的值
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 1){
      val log = args(0).asInstanceOf[Log]
      val deviceDict = args(1).asInstanceOf[Map[String,String]]

      //操作系统标签
      //client 设备类型 （1：android 2：ios 3：wp）如果获取不到就是4类型，4就是其他的
      val os = deviceDict.getOrElse(log.client.toString,deviceDict.get("4").get)
      map += (os -> 1)
      //联网方式标签
      //networkmannername 联网方式名称，如果没有就给NETWORKOTHER代表 其他
      val network = deviceDict.getOrElse(log.networkmannername,deviceDict.get("NETWORKOTHER").get)
       map += (network -> 1)
      //运营商的标签
      val isp = deviceDict.getOrElse(log.ispname,deviceDict.get("OPERATOROTHER").get)
    }
    map
  }
}
