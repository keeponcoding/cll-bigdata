package com.cll.flink.counter

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * @ClassName CounterApp
 * @Description 累加器
 * @Author cll
 * @Date 2020/2/29 4:08 下午
 * @Version 1.0
 **/
object CounterApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ele = env.fromElements("a", "b", "c", "d", "e", "f", "g")

    val result = ele.map(new RichMapFunction[String, String] {

      val counter = new IntCounter()

      // 将计数器添加到 RuntimeContext
      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("word_num", counter)
      }

      override def map(value: String): String = {
        // 取出计数器 进行操作
        getRuntimeContext.getAccumulator("word_num").add(1)
        value
      }
    }).setParallelism(10)

    result.writeAsText("cll-flink/out2")

    // 执行
    val jobExecutionResult:JobExecutionResult = env.execute(this.getClass.getSimpleName)
    // 取出计数器
    val cnt = jobExecutionResult.getAccumulatorResult[Int]("word_num")
    println(cnt)

  }

}
