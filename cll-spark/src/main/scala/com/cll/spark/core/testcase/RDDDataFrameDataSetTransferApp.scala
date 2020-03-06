package com.cll.spark.core.testcase

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @ClassName RDDDataFrameDataSetTransferApp
 * @Description RDD DataFrame DataSet 转换操作
 * @Author cll
 * @Date 2020/3/6 2:22 下午
 * @Version 1.0
 **/
object RDDDataFrameDataSetTransferApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val text = spark.sparkContext.textFile("cll-spark/data/stu.log")
    /*val rowRDD = text.map(x => {
      val a = x.split(",")
      Row(a(0).trim, a(1).trim.toInt, a(2).trim)
    })

    val schema = StructType(Array(StructField("name", StringType), StructField("age", IntegerType), StructField("addr", StringType)))
    val df = spark.createDataFrame(rowRDD, schema)
    df.select("name", "age", "addr").show()*/

    import spark.implicits._
    val df = text.map(x => {
      val a = x.split(",")
      People(a(0).trim, a(1).trim.toInt, a(2).trim)
    }).toDF()

    // DataFrame -> RDD
    df.rdd

    df.select("name", "age", "addr").show()

    // DataFrame -> DataSet
    val ds = df.as[People]
    ds.select("name", "age", "addr").show()
    // DataSet -> RDD
    ds.rdd

    /*import spark.implicits._
    // RDD -> DataSet
    val ds = text.map(x => {
      val a = x.split(",")
      People(a(0).trim, a(1).trim.toInt, a(2).trim)
    }).toDS()

    ds.select("name", "age", "addr").show()

    // DataSet -> DataFrame
    ds.toDF().select("name", "age", "addr").show()*/

    spark.stop()
  }

  case class People(name:String, age:Int, addr:String)

}

