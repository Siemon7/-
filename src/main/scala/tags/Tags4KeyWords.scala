package tags

import beans.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 5）关键词（标签格式：Kxxx->1）xxx为关键字。关键词个数不能少于3个字符，且不能超过8个字符；关键字中如包含”|”,则分割成数组，转化成多个关键字标签
  */
object Tags4KeyWords  extends  Tags{
  /**
    * 打标签的方法
    * 打关键字的标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] ={
    var map=Map[String,Int]()
    if(args.length > 0){
       val row = args(0).asInstanceOf[Row]
      val keywords = row.getAs[String]("keywords")
      if(StringUtils.isNotEmpty(keywords)){
        val fields = keywords.split("\\|")
//        for(word <- fields){
//          if(word.length >= 3 && word.length <= 8){
//            map +=("K".concat(word) -> 1)
//          }
//        }
        fields.filter( word =>{
          word.length >=3 && word.length <=8
        }).map( str =>{
         map +=("K".concat(str.replace(":",""))  -> 1)
        })
      }
    }
    map
  }
}
