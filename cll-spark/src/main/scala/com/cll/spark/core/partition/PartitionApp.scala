package com.cll.spark.core.partition

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName PartitionApp
  * @Description 分区器
  * @Author cll
  * @Date 2019-12-14 21:55
  * @Version 1.0
  **/
object PartitionApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    //
    val srcRdd = sc.parallelize(List(1,2,3,4,5,6,30,100,300),3)

    /*
     * HashPartitioner
     * 对 3(分区数) 取模
     * 1   % 3 = 1
     * 2   % 3 = 2
     * 3   % 3 = 0
     * 4   % 3 = 1
     * 5   % 3 = 2
     * 6   % 3 = 0
     * 30  % 3 = 0
     * 100 % 3 = 1
     * 300 % 3 = 0
     */
    srcRdd.zipWithIndex().groupByKey()
      .mapPartitionsWithIndex((index,partition) => {
        partition.map(x => s"分区是$index,元素是${x._1}")
      }).foreach(println)

    /*
     * RangePartitioner
     * 按顺序平均分
     * 如果元素的个数能够被分区数整除 那么 每个分区的元素的个数都是一样的
     * 如果不能被整除 那么 多余的元素会从小分区开始依次分配
     * 示例① 元素：1 2 3 4 5 6 30 100 300
     * 那么元素分区情况如下
     * 1   --> 0
     * 2   --> 0
     * 3   --> 0
     * 4   --> 1
     * 5   --> 1
     * 6   --> 1
     * 30  --> 2
     * 100 --> 2
     * 300 --> 2
     *
     * 示例② 元素：1 2 3 4 5 6 30 100 300 400 500
     * 那么元素分区情况如下
     * 1   --> 0
     * 2   --> 0
     * 3   --> 0
     * 4   --> 0
     * 5   --> 1
     * 6   --> 1
     * 30  --> 1
     * 100 --> 1
     * 300 --> 2
     * 400 --> 2
     * 500 --> 2
     * 也就是 0 1 分区分别多两个元素
     */
    srcRdd.zipWithIndex().sortByKey()
      .mapPartitionsWithIndex((index,partition) => {
        partition.map(x => s"分区是$index,元素是${x._1}")
      }).foreach(println)

    sc.stop()
  }

}
