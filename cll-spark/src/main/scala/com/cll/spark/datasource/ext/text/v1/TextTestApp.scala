package com.cll.spark.datasource.ext.text.v1

import org.apache.spark.sql.SparkSession

/**
  * @ClassName TextTextApp
  * @Description 测试自定义Spark外部数据源类
  * @Author cll
  * @Date 2019-12-05 23:06
  * @Version 1.0
  **/
object TextTestApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    /*
     * format 里面的路径写到 DefaultSource 类的 包名那一层即可
     * ★★★★★ 定义 DefaultSource 的时候  类名不要随意改动  框架底层会自动找format里面配置的包下面的DefaultSource类
     */
    spark.read.format("com.cll.bigdata.datasource.ext.text.v1")
        .option("path","cll-spark/data/emp.txt").load().show()

    spark.stop()
  }

}
