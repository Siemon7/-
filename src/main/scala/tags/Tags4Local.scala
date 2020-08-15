package tags

/**
  * 1）广告位类型（标签格式：LC03->1或者LC16->1）xx为数字，小于10 补0
  */
import beans.Log

object Tags4Local extends Tags {
  /**
    * 打标签的方法
    * 广告位的标签
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    if (args.length > 0) {
      //在scala中强制转换类型使用asInstanceOf
      val log = args(0).asInstanceOf[Log]
      //adspacetype广告位类型（1：banner 2：插屏 3：全屏）
      if (log.adspacetype != 0 && log.adspacetype != null) {
        log.adspacetype match {
          case x if x < 10 => map += ("LC0" + x -> 1)
          case x if x > 9 => map += ("LC" + x -> 1)
        }
      }
    }
    map
  }
}
