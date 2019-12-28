package com.cll.spark.project.offline

import org.apache.spark.sql.SparkSession

/**
  * @ClassName DFToMysql
  * @Description 将sparksql计算的结果集 推送到Mysql数据库
  * @Author cll
  * @Date 2019-12-28 21:47
  * @Version 1.0
  **/
object DFToMysql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val day = spark.sqlContext.getConf("spark.app.day")
    val hour = spark.sqlContext.getConf("spark.app.hour")
    val df = spark.sql(s"select * from cll.loginCount where day='${day}' and hour='${hour}'")
    //todo 先把MySQL中 day='${day}' and hour='${hour}'的数据进行删掉
    //方法一
    df.foreach(x=>{
      val project_id=x.getString(1)
      val countNum=x.getString(2)
      val day=x.getString(3)
      val hour=x.getString(4)
      //todo jdbc 批量插入
    })
    //方法二
    df.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()

    spark.stop()
  }

}
