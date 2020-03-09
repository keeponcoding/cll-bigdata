package com.cll.spark.core.accumulator

import org.apache.spark.sql.SparkSession

/**
 * @ClassName TestStringAccumulator
 * @Description
 *             测试自定义累加器 StringAccumulator
 * @Author cll
 * @Date 2020/3/9 2:47 下午
 * @Version 1.0
 **/
object TestStringAccumulator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val accu = new StringAccumulator
    // Accumulator must be registered before send to executor
    spark.sparkContext.register(accu)

    val rdd = spark.sparkContext.parallelize(Array("a", "b", "c", "d", "e", "f", "g"),3)
    rdd.foreach(x => {
      accu.add(x)
    })

    println(accu.value)

    spark.stop()
  }

}
