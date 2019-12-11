package com.cll.bigdata.core.custome

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName CustomeFunDealTextFile
  * @Description 自定义方法 处理读取textFile的数据
  * @Author cll
  * @Date 2019-12-03 08:04
  * @Version 1.0
  **/
object CustomeFunDealTextFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val accessRDD = sc.textFile("cll-spark/data/access.log")

    accessRDD.flatMap(MyMapFunc).map((_,1)).reduceByKey(_+_).foreach(println)

//    accessRDD.flatMap(x => x.split(",")).map((_,1)).reduceByKey(_+_).foreach(println)

    sc.stop()
  }

  // 简单的自定义方法 处理数据  按照,切分
  def MyMapFunc(str:String) ={
    str.split(",")
  }

}
