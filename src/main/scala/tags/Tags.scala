package tags

/**
  * Create by jenrey on 2018/5/16 14:48
  */
trait Tags {
  /**
    * 打标签的方法
    * 可以传多个对象
    * 因为有些标签传入的个数不确定，例如有些标签还可能需要传入映射文件(缓存文件)，所以需要 args:Any*
    */
  def makeTags(args:Any*):Map[String,Int]
}
