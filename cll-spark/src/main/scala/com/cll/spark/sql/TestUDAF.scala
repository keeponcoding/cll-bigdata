package com.cll.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @ClassName TestUDAF
 * @Description TODO
 * @Author cll
 * @Date 2020/8/27 4:10 下午
 * @Version 1.0
 **/
object TestUDAF {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    sc.read.text("cll-spark/data/data.txt")
        .createTempView("data")

    sc.udf.register("myCount", CountFunUDAF)

    sc.sql("select value,count(value) from data group by value").show()
    sc.sql("select value,myCount(value) from data group by value").show()

    sc.stop()
  }

  case class Word(w: String)

}
