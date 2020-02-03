package com.cll.spark.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName CheckPointSSApp
 * @Description
 *             该种方式 最大的弊端就是不能更改已上线的代码
 *             更改之后 程序就会报错 找不到 checkpoint 文件夹下的元数据信息
 *
 *             所以这种方式不能生产使用  只能自己测试使用
 * @Author cll
 * @Date 2020/2/3 9:14 下午
 * @Version 1.0
 **/
object CheckPointSSApp {

  val checkpointDirectory = "cll-spark/checkpoint/"

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()

  }

  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("hadoop000", 9999)

    val result = lines.flatMap(_.split(",")).map((_,1))
      .updateStateByKey(updateFunction)

    result.print()

    /*// output operation
    result.foreachRDD(rdd => {
      // foreachRDD 该算子运行在driver端

      rdd.foreachPartition(partition => {
        // foreachPartition 该算子运行在executor端
        // 获取数据库连接 最好使用连接池操作 使用完再放回连接池
        partition.foreach(pair => {
          // 拼接sql
        })
        // 执行sql
      })
    })*/

    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
    ssc
  }

  /**
   *
   * @param newValues 当前批次的值  数组
   * @param preValues 历史值  可能历史不存在 所以使用Option
   * @return
   */
  def updateFunction(newValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val newCount = newValues.sum

    val curr = preValues.getOrElse(0)

    Some(newCount + curr)
  }
}
