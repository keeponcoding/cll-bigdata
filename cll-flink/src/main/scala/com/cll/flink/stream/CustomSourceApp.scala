package com.cll.flink.stream

import com.cll.flink.stream.source.AccessSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ClassName CustomSourceApp
 * @Description 接收 自定义数据源 的数据
 * @Author cll
 * @Date 2020/2/10 3:19 下午
 * @Version 1.0
 **/
object CustomSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new AccessSource)
      /*
       * java.lang.IllegalArgumentException: Source: 1 is not a parallel source
       * SourceFunction 的并行度只能为1
       */
      .setParallelism(1)
      .print()

    env.execute(this.getClass.getSimpleName)
  }

}
