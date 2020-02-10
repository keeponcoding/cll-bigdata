package com.cll.flink.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @ClassName StreamingJob
 * @Description flink 流处理
 * @Author cll
 * @Date 2020/2/10 10:29 上午
 * @Version 1.0
 **/
object StreamingV1Job {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收数据
    val ds: DataStream[String] = env.socketTextStream("hadoop000", 9999)

    // transformation
    ds.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print("cll")
      // 设置并行度
      .setParallelism(1)

    // run job
    env.execute(this.getClass.getSimpleName)

  }

}
