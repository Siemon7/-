package tags

/**
  * 3）渠道（标签格式：CNxxxx->1）xxxx为渠道ID
  */
import beans.Log
import org.apache.commons.lang.StringUtils

object Tags4Channel extends  Tags{
  /**
    * 打标签的方法
    * 打渠道的标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 0){
       val log = args(0).asInstanceOf[Log]
      if(StringUtils.isNotEmpty(log.channelid)){
        map += ("CN".concat(log.channelid) -> 1)
      }
    }
    map
  }
}
