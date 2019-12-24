package com.cll.spark.core.testcase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName Line2ColApp
  * @Description 行专列练习
  *
  * 表tmp两个字段 user,profile
  * user profile
  * abc  key1:value,key2:value2
  * def  key1:value,key2:value2,key3:value3,key4:value4
  * xyz  key1:value
  * 转换成
  * user profile_key profile_value
  * abc   key1         value
  * abc   key2         value2
  * def   key1         value
  * def   key2         value2
  * def   key3         value3
  * def   key4         value4
  * xyz   key1         value
  * @Author cll
  * @Date 2019-12-24 19:50
  * @Version 1.0
  **/
object Line2ColApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val upRDD = sc.textFile("cll-spark/data/user_profile.txt")

    upRDD.flatMap(line => {
      val word = line.split(" ")
      val user = word(0)
      val kvs = word(1)
      kvs.split(",").map(kv => {
        val s_kv = kv.split(":")
        (user,s_kv(0),s_kv(1))
      })
    }).foreach(println)

    sc.stop()
  }

}
