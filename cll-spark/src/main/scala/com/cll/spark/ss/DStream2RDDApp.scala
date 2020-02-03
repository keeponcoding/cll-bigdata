package com.cll.spark.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @ClassName DStream2RDDApp
 * @Description DStream 转换成 RDD
 * @Author cll
 * @Date 2020/2/3 10:02 下午
 * @Version 1.0
 **/
object DStream2RDDApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("hadoop000", 9999)

    // 黑名单
    val blacks = new ListBuffer[(String,Boolean)]()
    blacks.append(("aaa",true))
    val blacksRDD = ssc.sparkContext.parallelize(blacks)

    val result = lines.map(x => (x.split(",")(0),x))
      .transform(rdd => {
      // TODO ...
        rdd.leftOuterJoin(blacksRDD)
          .filter(_._2._2.getOrElse(false) != true)
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
