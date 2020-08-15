package utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object JedisPools {

    private val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "192.168.28.12", 6379,900000000)


    def getJedis() = jedisPool.getResource

}
