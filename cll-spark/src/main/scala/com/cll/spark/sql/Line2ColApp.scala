package com.cll.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ClassName Line2ColApp
  * @Description 行转列
  *
  * 表tmp两个字段 user,profile
  * user profile
  * abc  key1:value,key2:value2
  * def  key1:value,key2:value2,key3:value3,key4:value4
  * xyz  key1:value
  * 转换成
  * user profile_key profile_value
  * abc   key1         value
  * abc   key2         value2
  * def   key1         value
  * def   key2         value2
  * def   key3         value3
  * def   key4         value4
  * xyz   key1         value
  * @Author cll
  * @Date 2019-12-24 20:28
  * @Version 1.0
  **/
object Line2ColApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val kvRDD = spark.sparkContext.textFile("cll-spark/data/user_profile.txt")

    val rowRDD = kvRDD.map(line => {
      val word = line.split(" ")
      Row(word(0),word(1))
    })

    val schema = StructType(Array(StructField("user",StringType),StructField("profile",StringType)))

    spark.createDataFrame(rowRDD,schema).createTempView("user_profile")

    spark.sql(
      """
        |select
        |  t.user
        |  ,split(t.profile_kv,':')[0] profile_key
        |  ,split(t.profile_kv,':')[1] profile_value
        |from
        |(
        |  select
        |    a.user
        |    ,explode(split(a.profile,',')) profile_kv
        |  from user_profile a
        |) t
      """.stripMargin).show()

  }


}
