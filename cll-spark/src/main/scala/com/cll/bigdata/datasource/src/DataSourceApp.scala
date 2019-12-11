package com.cll.bigdata.datasource.src

import org.apache.spark.sql.SparkSession

/**
  * @ClassName DataSourceApp
  * @Description 数据源   原生自带的
  * @Author cll
  * @Date 2019-12-05 22:11
  * @Version 1.0
  **/
object DataSourceApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    // jdbc
    jdbc(spark)

    spark.stop()
  }

  /*
   * jdbc
   *
   * issue [SPARK-24423]
   *
   * 不允许同时指定 dbtable query
   *
   * 【全表扫描】
   * dbtable -> schema.tablename 获取全表数据
   * 等同于
   * query -> select * from schema.tablename
   *
   * ★★★★★
   * 【指定条件】
   * dbtable -> (select * from schema.tablename where xxx) as tmp 获取指定条件数据
   * 等同于
   * query -> select * from schema.tablename where xxx
   *
   */
  def jdbc(spark:SparkSession) = {
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop000:3306")
      //.option("dbtable", "hive_metadata.tbls")
      //.option("dbtable", "(select * from hive_metadata.tbls where tbl_id = 3) as tmp") // 获取指定条件数据
      .option("user", "root")
      .option("password", "123456")
      .option("query", "select * from hive_metadata.tbls where tbl_id = 3") // 可以读取某几列并且加上筛选条件
      .load()
    df.show(false)

//    spark.read.jdbc("","","","")

  }

}
