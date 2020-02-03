package com.cll.spark.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @ClassName JedisUtil
 * @Description jedis工具类
 * @Author cll
 * @Date 2020/2/3 9:43 下午
 * @Version 1.0
 **/
object JedisUtil {

    val poolConfig = new JedisPoolConfig
    poolConfig.setMaxTotal(2000)
    poolConfig.setMaxIdle(1000)
    poolConfig.setTestOnBorrow(true)

    private val pool = new JedisPool(poolConfig, "hadoop000")

    def jedis = pool.getResource
}
