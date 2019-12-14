package com.cll.bigdata.core.compare

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName ReduceAndGroupByKeyApp
  * @Description reduceByKey & groupByKey
  * @Author cll
  * @Date 2019-12-14 20:55
  * @Version 1.0
  **/
object ReduceAndGroupByKeyApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val scrRdd = sc.parallelize(List(
      "10000,一起看|电视剧|军旅|士兵突击,1,0", //uid,导航,展示,点击
      "10000,一起看|电视剧|军旅|士兵突击,1,1",
      "10001,一起看|电视剧|军旅|我的团长我的团,1,1"
    ))

    val kvRdd = scrRdd.flatMap(a => {
      // 先根据,切分
      val b = a.split(",")
      val uid = b(0)
      val navicate = b(1)
      val show = b(2).toInt
      val click = b(3).toInt
      navicate.split("\\|").map(c => {
        ((uid,c),(show,click))
      })
    })

    /*
     * reduceByKey 方式实现
     * combineByKeyWithClassTag[C](createCombiner: V => C,mergeValue: (C, V) => C,mergeCombiners: (C, C) => C,partitioner: Partitioner,
     *      mapSideCombine: Boolean = true,serializer: Serializer = null)
     *
     * mapSideCombine: Boolean = true map端聚合
     * 默认为true reduceByKey 会开启map端聚合操作 也就是本地聚合 预聚合之后 在每个task对于相同的key的数据只有一条
     */
    kvRdd.reduceByKey((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    }).foreach(println)

    /*
     * groupByKey 方式实现
     * mapValues key不变 动value
     * combineByKeyWithClassTag[CompactBuffer[V]](createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
     * mapSideCombine = false map端聚合 默认为false 也就是 groupByKey 默认不进行map端聚合
     */
    kvRdd.groupByKey().mapValues(x => {
      (x.map(_._1).sum,x.map(_._2).sum)
    }).foreach(println)

    sc.stop()
  }

}
