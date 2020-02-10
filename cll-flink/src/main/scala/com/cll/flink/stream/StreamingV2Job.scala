package com.cll.flink.stream

import com.cll.flink.bean.Domain.WC
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @ClassName StreamingJob
 * @Description flink 流处理
 * @Author cll
 * @Date 2020/2/10 10:29 上午
 * @Version 1.0
 **/
object StreamingV2Job {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收数据
    val ds: DataStream[String] = env.socketTextStream("hadoop000", 9999)

    // transformation
    ds.flatMap(_.split(","))
      .map(x => WC(x,1))
      // 指定字段 增加可读性
      //.keyBy("word")
      // 生产实际使用 使用方便 可读性强 避免字段编写错误
      .keyBy(_.word)
      .sum("count")
      .print("cll")
      // 设置并行度
      .setParallelism(1)

    // run job
    env.execute(this.getClass.getSimpleName)

  }

}
