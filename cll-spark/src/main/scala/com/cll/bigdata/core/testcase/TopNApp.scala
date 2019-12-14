package com.cll.bigdata.core.testcase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName TopNApp
  * @Description 分组排序/组内排序
  * @Author cll
  * @Date 2019-12-14 21:05
  * @Version 1.0
  **/
object TopNApp {

  val TOPN = 2

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 加载原数据
    val srcRdd = sc.textFile("cll-spark/data/site.log")

    // 组合成 kv 结构的数据
    val processRdd = srcRdd.map(lines => {
      val info = lines.split(",")
      ((info(0), info(1)), 1)
    })

    /*
     * ①
     * 该方式小数据量还行
     * 一旦数据量大 就会导致oom
     * 生产不建议使用该方式进行计算
     */
    /*processRdd
      .reduceByKey(_+_) // 聚合
      .groupBy(_._1._1) // 按照域名分组
      .mapValues(x => {
        x.toList // 此处 toList 是scala的 只可用于测试小数据量 安全隐患很大
          .sortBy(-_._2) // 排序
          .map(x => (x._1._2,x._2)) // 组合 (url,num) url以及对应的访问量
          .take(TOPN) // 取出前三
      })
      .foreach(println)*/

    /*
     * ②
     * TODO 分而治之的思想 处理单个的域名
     */
    //val domains = Array("www.baidu.com","www.jd.com","www.google.com")
    // collect 存在性能问题 待优化...
    val domains = processRdd.map(_._1._1).distinct().collect()
    // 遍历域名 单个进行取出topn
    // RDD 操作内部不能使用另外一个 RDD  所以 val domains = processRdd.map(_._1._1).distinct() 这种情况下是会报错的
    domains.foreach(a => {
      processRdd
        .filter(_._1._1 == a) // 过滤域名 取出待计算域名
        .reduceByKey(_+_) // 聚合
        .sortBy(-_._2) // 按照聚合的结果进行排序
        .take(TOPN) // 取出前n个
        .foreach(println)
    })

    sc.stop()
  }

}
