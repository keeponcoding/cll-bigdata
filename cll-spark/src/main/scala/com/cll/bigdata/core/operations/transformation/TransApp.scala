package com.cll.bigdata.core.operations.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName TransApp
  * @Description transformation operation
  * @Author cll
  * @Date 2019-12-03 09:02
  * @Version 1.0
  **/
object TransApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("cll-spark/data/access.log")

    /*
     * map & flatMap
     */
    rdd.map(x => {
      val a = x.split(",")
      (a(0),a(1),a(2))
    }).foreach(println)
    println("~~~~~~~~~~~~~~~~~~~~~~~~")
    rdd.flatMap(x => x.split(",")).foreach(println)


    sc.stop()
  }

}
