package com.cll.flink.stream

import com.cll.flink.stream.custom.{AccessSource, ParallelAccessSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @ClassName CustomSourceApp
 * @Description 接收 自定义数据源 的数据
 * @Author cll
 * @Date 2020/2/10 3:19 下午
 * @Version 1.0
 **/
object CustomParallelSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new ParallelAccessSource)
      .setParallelism(2)
      .print()

    env.execute(this.getClass.getSimpleName)
  }

}
