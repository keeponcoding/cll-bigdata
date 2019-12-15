package com.cll.spark.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ClassName FlumeSparkStreamingApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-10 10:33
  * @Version 1.0
  **/
object FlumeSparkStreamingApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.socketTextStream("hadoop000",9999)

    ssc.start() // 启动
    ssc.awaitTermination()
  }

}
