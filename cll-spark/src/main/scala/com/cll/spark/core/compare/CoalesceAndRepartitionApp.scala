package com.cll.spark.core.compare

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName CoalesceAndRepartitionApp
  * @Description Coalesce Repartition 作比较
  * @Author cll
  * @Date 2019-12-14 15:48
  * @Version 1.0
  **/
object CoalesceAndRepartitionApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // :_* 代表
    val rdd = sc.parallelize(List(1 to 9:_*),3)

    // repartition call coalesce(numPartitions, shuffle = true)
    // 所以默认 shuffle = true 会产生shuffle
    rdd.repartition(4)


    // coalesce(numPartitions: Int, shuffle: Boolean = false,...)    所以默认 shuffle = false 不会产生shuffle
    // This results in a narrow dependency 窄依赖
    // 此时指定的分区数为4    4大于原分区数3   此时分区数是不变化的
    rdd.coalesce(4)
    // 如果想使上面生效 代码如下
    rdd.coalesce(4,true)


    // 此时指定的分区数为2    2小于原分区数3   此时分区数会发生变化
    rdd.coalesce(2)

    sc.stop()
  }

}
