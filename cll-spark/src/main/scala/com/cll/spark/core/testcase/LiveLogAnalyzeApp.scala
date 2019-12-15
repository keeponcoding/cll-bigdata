package com.cll.spark.core.testcase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName LiveLogAnalyzeApp
  * @Description 直播日志分析
  * 示例数据如下
  * sc.parallelize(List(
  * "10000,一起看|电视剧|军旅|士兵突击,1,0", //uid,导航,展示,点击
  * "10000,一起看|电视剧|军旅|士兵突击,1,1",
  * "10001,一起看|电视剧|军旅|我的团长我的团,1,1"
  * ))
  *
  * 结果如下：
  * ((10000,军旅),(2,1))
  * ((10000,电视剧),(2,1))
  * ((10001,军旅),(1,1))
  * ((10000,一起看),(2,1))
  * ((10001,我的团长我的团),(1,1))
  * ((10001,一起看),(1,1))
  * ((10001,电视剧),(1,1))
  * ((10000,士兵突击),(2,1))
  * @Author cll
  * @Date 2019-12-14 16:31
  * @Version 1.0
  **/
object LiveLogAnalyzeApp {

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

    // reduceByKey 方式实现
    /*kvRdd.reduceByKey((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    }).foreach(println)*/

    // groupByKey 方式实现
    // mapValues key不变 动value
    kvRdd.groupByKey().mapValues(x => {
      (x.map(_._1).sum,x.map(_._2).sum)
    }).foreach(println)

    sc.stop()
  }

}
