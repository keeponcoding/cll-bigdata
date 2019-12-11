package com.cll.bigdata.core.operations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName LineageApp
  * @Description RDD 依赖关系
  * @Author cll
  * @Date 2019-12-09 21:18
  * @Version 1.0
  **/
object LineageApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    /*val a = sc.parallelize(List(1,2,3,4,5,6,7))
    val b = a.map(_*2)
    val c = b.filter(_ > 4)
    println(c.toDebugString)*/

    // 面试题：下面操作 创建了多少种RDD
    /*
     * (1) ShuffledRDD[4] at reduceByKey at LineageApp.scala:27 []
     * +-(1) MapPartitionsRDD[3] at map at LineageApp.scala:26 []
     *    |  MapPartitionsRDD[2] at flatMap at LineageApp.scala:25 []
     *    |  cll-spark/data/wc.data MapPartitionsRDD[1] at textFile at LineageApp.scala:24 []
     *    |  cll-spark/data/wc.data HadoopRDD[0] at textFile at LineageApp.scala:24 []
     */
    println(sc.textFile("cll-spark/data/wc.data") // HadoopRDD MapPartitionsRDD
      .flatMap(a => a.split(",")) // MapPartitionsRDD
      .map((_, 1)) // MapPartitionsRDD
      .reduceByKey(_ + _) // ShuffledRDD
      .toDebugString)

    sc.stop()
  }

}
