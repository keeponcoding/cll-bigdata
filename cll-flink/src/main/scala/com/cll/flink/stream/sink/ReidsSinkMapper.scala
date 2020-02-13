package com.cll.flink.stream.sink

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @ClassName ReidsSinkApp
 * @Description 写入redis
 * @Author cll
 * @Date 2020/2/13 9:08 上午
 * @Version 1.0
 **/
class ReidsSinkMapper extends RedisMapper[(String, Double)]{
  // 命令行描述
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")
  }

  // 从数据中获取key
  override def getKeyFromData(data: (String, Double)): String = {
    data._2 + ""
  }

  // 从数据中获取value
  override def getValueFromData(data: (String, Double)): String = {
    data._1
  }
}
