package com.cll.spark.core.testcase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName GroupSumApp
  * @Description 分组求和
  *             数据如下：
  *             a,1,3
  *             a,2,4
  *             b,1,1
  *             输出结果：
  *             a,3,7
  *             b,1,1
  * @Author cll
  * @Date 2019-12-14 16:11
  * @Version 1.0
  **/
object GroupSumApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 造数据
    val srcRdd = sc.parallelize(List("a,1,3","a,2,4","b,1,1"))

    val kvRdd = srcRdd.map(a => {
      val b = a.split(",")
      (b(0), (b(1).toInt, b(2).toInt))
    })

    kvRdd.reduceByKey((x,y) => {
      (x._1+y._1, x._2+y._2)
    }).map(a => {
      (a._1,a._2._1,a._2._2)
    }).foreach(println)

    sc.stop()
  }

}
