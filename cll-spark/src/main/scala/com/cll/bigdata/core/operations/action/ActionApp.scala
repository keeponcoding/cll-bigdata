package com.cll.bigdata.core.operations.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName ActionApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-09 22:01
  * @Version 1.0
  **/
object ActionApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)



    sc.stop()
  }

}
