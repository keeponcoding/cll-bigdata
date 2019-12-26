package com.cll.spark.scala

/**
  * @ClassName ListApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-25 21:17
  * @Version 1.0
  **/
object ListApp {

  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5)

    // slice 类似于  substring  截取
    println(list.slice(0,3))

  }

}
