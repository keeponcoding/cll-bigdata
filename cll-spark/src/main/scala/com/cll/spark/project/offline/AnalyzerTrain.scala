package com.cll.spark.project.offline

import org.apache.spark.sql.SparkSession

/**
  * @ClassName AnalyzerTrain
  * @Description
  *             需求：每个app每小时有多少次登陆
  *
  *             问题1：小文件
  *             spark.sql.shuffle.partitions=200 spark shuffle read  无法动态
  *             建议新启动作业处理小文件
  *
  *             问题2：重跑
  *             利用overwrite特性 全分区覆盖
  *
  *             问题3：数据倾斜
  *             group by 会产生shuffle
  *             热点数据 进行加随机数 打散
  * @Author cll
  * @Date 2019-12-28 18:25
  * @Version 1.0
  **/
object AnalyzerTrain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport() // 开启支持hive
      .getOrCreate()

    val day = spark.sqlContext.getConf("spark.app.day")
    val hour = spark.sqlContext.getConf("spark.app.hour")
    val anaDF = spark.sql(s"select project_id,day,hour,count(1) from cll.appLog where day='${day}' and " +
      s"hour='${hour}' group by project_id,day,hour")
    anaDF.createOrReplaceTempView("loginCount")
    spark.sql(
      s"""
         |create external table if not exist rz.loginCount(
         |project_id string,
         |countNum int
         |)
         |partition by (`day` string,`hour` string)
         |stored as parquet
         |location '................'
       """.stripMargin)
    spark.sql("insert overwrite table cll.loginCount select project_id,countNum,day,hour from loginCount")

    spark.stop()
  }

}
