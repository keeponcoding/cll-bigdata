package com.cll.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName LeadShuffleApp
  * @Description 测试哪些算子会产生shuffle
  * @Author cll
  * @Date 2019-12-14 16:00
  * @Version 1.0
  **/
object LeadShuffleApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1 to 9),3)

    /*
     * repartition operations
     * 1.repartition
     * 2.coalesce
     * ...
     */


    /*
     * *ByKey operations
     * 1.groupByKey
     * 2.reduceByKey
     * ...
     */


    /*
     * join operations
     * 1.cogroup
     * 2.join
     * ...
     */


    sc.stop()
  }

}
