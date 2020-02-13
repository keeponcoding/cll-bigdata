package com.cll.flink.stream

import com.cll.flink.stream.sink.ReidsSinkMapper
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

/**
 * @ClassName Result2RedisApp
 * @Description addSink 将结果写入redis
 * @Author cll
 * @Date 2020/2/13 9:16 上午
 * @Version 1.0
 **/
object Result2RedisApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("cll-flink/data/access.log")

    val stream = text.map(x => {
      val a = x.split(",")
      (a(1), a(2).toDouble)
    }).keyBy(0).sum(1)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop000").build()
    stream.addSink(new RedisSink[(String, Double)](conf, new ReidsSinkMapper))

    env.execute(this.getClass.getSimpleName)
  }

}
