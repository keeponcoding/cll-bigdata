package com.cll.spark.core.operations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName BroadcastApp
  * @Description 广播变量
  * @Author cll
  * @Date 2019-12-15 15:21
  * @Version 1.0
  **/
object BroadcastApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // 最佳实践 待广播的rdd.collectAsMap
    val rdd1 = sc.parallelize(Array(("1000","zhangsan"),("1001","lisi"))).collectAsMap()
    val rdd2 = sc.parallelize(Array(("1000","shanghai"),("1001","beijing"),("1003","shenzhen")))
    // 将rdd1广播出去
    val rdd1_bc = sc.broadcast(rdd1)
    rdd2.map(x => (x._1,x)).mapPartitions(a => {
      // 获取广播变量的值
      val rdd1_value = rdd1_bc.value
      // 匹配
      for ((k,v) <- a if(rdd1_value.contains(k)))
          yield (k, rdd1_value.get(k).getOrElse(""), v._2)
    }).foreach(println)

    sc.stop()

  }

}
