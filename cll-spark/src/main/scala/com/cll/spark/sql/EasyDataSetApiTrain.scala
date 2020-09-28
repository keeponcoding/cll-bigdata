package com.cll.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

/**
 * @ClassName EasyDataSetApiTrain
 * @Description TODO
 * @Author cll
 * @Date 2020/9/28 9:09 上午
 * @Version 1.0
 **/
object EasyDataSetApiTrain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sc = spark.sparkContext

    val sales = spark.createDataFrame(
      Seq(
        ("Warsaw", 2016, 100), ("Warsaw", 2017, 200), ("Warsaw", 2015, 100), ("Warsaw", 2017, 200), ("Beijing", 2017, 200),
        ("Beijing", 2016, 200), ("Beijing", 2015, 200), ("Beijing", 2014, 200), ("Warsaw", 2014, 200), ("Boston", 2017, 50),
        ("Boston", 2016, 50), ("Boston", 2015, 50), ("Boston", 2014, 150)
      )
    ).toDF("city", "year", "amount")

    /*
     * select
     * 列名称可以是字符串，这种形式无法对列名称使用表达式进行逻辑操作
     * 使用col函数，可以直接对列进行一些逻辑操作
     */
    println(" select col example: ")
    sales.select("city","year","amount").show(1)
    sales.select(col("city"),col("amount")+1).show(1)

    /*
     * expr
     * 参数是字符串，且直接可以使用表达式
     * 也可以使用select+expr函数来替代
     */
    println(" select expr example: ")
    sales.selectExpr("city","year as date","amount+1").show(5)
    sales.select(expr("city"),expr("year as date"),expr("amount+1")).show(5)

    /*
     * filter
     * 参数可以是与col结合的表达式，参数类型为row返回值为boolean的函数，字符串表达式
     */
    println(" filter example: ")
    sales.filter(col("amount")>150).show()
    sales.filter(row=>{ row.getInt(2)>150}).show(10)
    sales.filter("amount > 150 ").show(10)

    /*
     * withColumn
     * 假如列，存在就替换，不存在新增 withColumnRenamed 对已有的列进行重命名
     */
    println(" withColumn example: ")
     //相当于给原来amount列，+1
    sales.withColumn("amount",col("amount")+1).show()
    // 对amount列+1，然后将值增加到一个新列 amount1
    sales.withColumn("amount1",col("amount")+1).show()
    // 将amount列名，修改为amount1
    sales.withColumnRenamed("amount","amount1").show()

    /*
     * sortwithinpartition
     * 分区内部排序  局部有序    区别于 order by / sort by
     */
    println(" sortwithinpartition example: ")
    sales.sortWithinPartitions(col("year").desc,col("amount").asc).show()
    sales.sortWithinPartitions("city","year").show()

    /*
     * printSchema
     * 输出dataset的schema信息
     */
    println(" printSchema example: ")
    sales.printSchema()

    /*
     * explain
     * 打印执行计划
     * 还可以传入 Boolean 值，决定是否需要打印扩展信息
     * since 1.6
     */
    sales.sortWithinPartitions(col("year").desc,col("amount").asc).explain()


    sc.stop()
  }

}
