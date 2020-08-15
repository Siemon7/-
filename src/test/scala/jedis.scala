import org.junit.Test
import redis.clients.jedis.Jedis
import utils.JedisPools

object jedis {
  @Test
  def main(args: Array[String]): Unit = {


    val jedis: Jedis = JedisPools.getJedis()
    jedis.set("bb", "22")
  }
}
