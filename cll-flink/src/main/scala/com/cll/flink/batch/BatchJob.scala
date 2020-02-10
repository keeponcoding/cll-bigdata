package com.cll.flink.batch

import org.apache.flink.api.scala._

/**
 * @ClassName BatchJob
 * @Description flink 批处理
 * @Author cll
 * @Date 2019/12/10 10:05 上午
 * @Version 1.0
 **/
object BatchJob {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds: DataSet[String] = env.readTextFile("cll-flink/data/wc.data")

    ds.flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()

  }

}
