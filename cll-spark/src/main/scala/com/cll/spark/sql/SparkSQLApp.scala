package com.cll.spark.sql

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * @ClassName SparkSQLApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-17 14:56
  * @Version 1.0
  **/
object SparkSQLApp {

  /**
    * text
    * @param spark
    */
  def text(spark:SparkSession) = {
    import spark.implicits._
    val file_path = "cll-spark/data/emp.txt"

    //val df1 = spark.read.text(file_path)
    val df2 = spark.read.format("text").load(file_path)

    df2.map(row => {
      val a = row.toString().split(",")
      Emp(a(0),a(1),a(2),a(3),a(4))
    }).show()
  }

  /**
    * json
    * @param spark
    */
  def json(spark: SparkSession) = {
    val file_path = "cll-spark/data/stu.json"
    //spark.read.json(file_path)
    val df = spark.read.format("json").load(file_path)

    // 注册临时表
    df.registerTempTable("stu")

    // 打印schema
    df.printSchema()

    // 挑选指定的列
    df.select("name").show()

    // 过滤
    val addr = "shenzhen"
    df.filter("age > 20").show()
    df.filter(s"addr = '$addr' ").show()

    // 聚合
    val frame = df.groupBy(new Column("addr"))sum("age")
    frame.show()

    spark.sql("select name,age,addr from stu").show()
  }

  /**
    * csv
    * @param spark
    */
  def csv(spark:SparkSession) = {
    val file_path = "cll-spark/data/people.csv"
    //spark.read.csv(file_path)
    val df = spark.read
      .format("csv")
      // 以下查找 org.apache.spark.sql.execution.datasources.csv.CSVOptions
      .option("sep",";")          // 指定分隔符 默认为 ,
      .option("inferSchema",true) // 自动推导字段类型 默认为false 也就是所有字段默认为String
      .option("header",true)      // 数据中第一行存在schema 默认为false 默认所有的行都为数据
      .load(file_path)

    df.printSchema()

    df.show()
  }

  def main(args: Array[String]): Unit = {
    /*
     * 2.0.0 之前的版本  new org.apache.spark.sql.SQLContext
     * @since 2.0.0 SparkSession
     */
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      //.enableHiveSupport() // 开启支持hive
      .getOrCreate()

    // 文本
    //text(spark)

    // json
    //json(spark)

    // csv
    csv(spark)

    spark.stop()
  }

  case class Emp(empCode:String, name:String, gender:String, salary:String, bonus:String)

}
