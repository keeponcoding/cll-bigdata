package com.cll.spark.core.compare

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName MapAndMapPartitionApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-15 09:46
  * @Version 1.0
  **/
object MapAndMapPartitionApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // map vs mapPartition

    sc.stop()
  }

}
