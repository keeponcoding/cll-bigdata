package com.cll.spark.scala

/**
  * @ClassName MaxValueApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-17 11:19
  * @Version 1.0
  **/
object MaxValueApp {

  def main(args: Array[String]): Unit = {
    val num = Array(1,32,3,22,12,93,399,45)
    var maxNum = 0

    // 取最大值

    // 方式一
    for (n <- num) {
      maxNum = Math.max(maxNum,n)
    }
    println(maxNum)

    // 方式二
    maxNum = num.foldLeft(Integer.MIN_VALUE){Math.max}
    println(maxNum)
  }

}
