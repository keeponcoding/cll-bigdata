package com.cll.spark.datasource.ext

import org.apache.spark.sql.SparkSession

/**
  * @ClassName CustomeExDataSource
  * @Description 自定义外部数据源
  * @Author cll
  * @Date 2019-12-05 20:26
  * @Version 1.0
  **/
object CustomeExtDataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    spark.read.format("")
      .load("")

    spark.stop()
  }

}
