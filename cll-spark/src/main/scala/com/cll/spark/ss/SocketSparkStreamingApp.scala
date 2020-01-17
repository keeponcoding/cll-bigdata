package com.cll.spark.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * @ClassName SparkStreamingApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-10 10:33
  * @Version 1.0
  **/
object SocketSparkStreamingApp {

  val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    // 获取 SparkContext
    // ssc.sparkContext

    val lines = ssc.socketTextStream("hadoop000",9999)
    log.warn("lines:{"+lines+"}")
    val words = lines.flatMap(a => a.split(",")).persist()
    log.warn("words:{"+words+"}")
    val pairs = words.map((_,1))
    log.warn("pairs:{"+pairs+"}")
    val result = pairs.reduceByKey(_+_)
    result.print()

    ssc.start() // 启动
    ssc.awaitTermination()

    // 只停止 StreamingContext 不停止 SparkContext
    // ssc.stop(false)
    // StreamingContext 停止之后 SparkContext 没有停止得话 还可以用 SparkContext 继续创建一个新的 StreamingContext
    // 同一时间 只能存在一个 StreamingContext
  }

}
