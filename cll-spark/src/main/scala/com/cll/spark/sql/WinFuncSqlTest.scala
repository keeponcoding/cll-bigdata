package com.cll.spark.sql

import org.apache.spark.sql.{Row, SparkSession}

/**
 * @ClassName WinFuncSqlTest
 * @Description TODO
 * @Author cll
 * @Date 2020/9/19 3:27 下午
 * @Version 1.0
 **/
object WinFuncSqlTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("aaa")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext.parallelize(Array(
      "1,zhangsan,10,20,30",
      "2,lisi,40,50,60",
      "3,wanger,70,80,90",
      "4,mazi,11,22,33",
      "5,xiaoming,110,120,130"
    )).map(a => {
      val b = a.split(",")
      Student(b(0),b(1),b(2),b(3),b(4))
    }).toDF

    /*spark.createDataFrame(
      Seq(
        ("1", "zhangsan", 10, 20, 30),
        ("2", "lisi", 40, 50, 60),
        ("3", "wanger", 70, 80, 90),
        ("4", "mazi", 11, 22, 33),
        ("5", "xiaoming", 110, 120, 130)
      )
    ).toDF("id", "name", "yw", "sx", "yy")*/


    df.createOrReplaceTempView("stu")

    // df.show(false)

    val df1 = spark.sql(
      """
        |select
        |id
        |,name
        |,stack(3,'语文',`yw`,'数学',`sx`,'英语',`yy`) as (`xk`,`score`)
        |from stu
        |""".stripMargin)
    df1.createOrReplaceTempView("stu_df1")
    df1.show(false)

    println("----------------------------------------")

    spark.sql(
      """
        |select
        |*
        |from stu_df1
        |""".stripMargin)

    /*spark.sql(
      """
        |select
        |unix_timestamp(now())
        |""".stripMargin).show()*/

    /*val rdd = spark.sparkContext.parallelize(
      Array(
        "a,2020-01-01,100",
        "a,2020-01-01,200",
        "a,2020-01-01,300",
        "b,2020-01-01,100",
        "b,2020-01-01,100",
        "b,2020-01-02,100",
        "b,2020-01-02,100",
        "c,2020-01-01,200",
        "c,2020-01-01,300",
        "c,2020-01-02,400"
      )
    )

    import spark.implicits._

    val df = rdd.map(a => {
      val b = a.split(",")
      UserInfo(b(0),b(1),b(2))
    }).toDF

    df.createOrReplaceTempView("user_info")

    spark.sql(
      """
        |select
        |user
        |,time
        |,amount
        |,sum(amount) over(order by user,time) acc_amount
        |from
        |(
        |  select
        |    user
        |    ,time
        |    ,sum(amount) amount
        |  from user_info
        |  group by user,time
        |)
        |""".stripMargin).show()

    spark.sql(
      """
        |select
        |user
        |,time
        |,amount
        |,sum(amount) over(order by user,time rows between unbounded preceding and current row) acc_amount
        |from
        |(
        |  select
        |    user
        |    ,time
        |    ,sum(amount) amount
        |  from user_info
        |  group by user,time
        |)
        |""".stripMargin).show()*/

    /*val rdd = spark.sparkContext.parallelize(
      Array(
      "1,2020-01-01,20",
      "1,2020-01-02,31",
      "1,2020-01-03,19",
      "1,2020-01-09,50",
      "2,2020-01-02,31",
      "2,2020-02-03,22",
      "3,2020-01-04,34",
      "3,2020-01-06,17",
      "3,2020-01-07,60",
      "3,2020-02-12,15",
      "3,2020-02-23,27"
      ))
    import spark.implicits._
    val df = rdd.map(a => {
      val b = a.split(",")
      UserInfo(b(0),b(1),b(2))
    }).toDF()

    df.show()

    df.createOrReplaceTempView("user_info")

    spark.sql(
      """
        |select
        |  user
        |  ,mon
        |  ,pv
        |  ,sum(pv) over(partition by user order by mon) acc_pv
        |  from
        |  (
        |    select
        |      user
        |      ,substr(time,1,7) mon
        |      ,sum(t.pv) pv
        |    from user_info t
        |    group by user,substr(time,1,7)
        |  )
        |order by user
        |""".stripMargin).show()

    spark.sql(
      """
        |select
        |  user
        |  ,mon
        |  ,pv
        |  ,sum(pv) over(order by mon) acc_pv
        |  from
        |  (
        |    select
        |      user
        |      ,substr(time,1,7) mon
        |      ,sum(t.pv) pv
        |    from user_info t
        |    group by user,substr(time,1,7)
        |  )
        |order by mon
        |""".stripMargin).show()*/


    spark.stop()

  }

  case class UserInfo(user:String, time:String, amount:String)
  case class Fruits(name:String, _2010:String, _2011:String,_2012:String)
  case class Student(id:String, name:String, yw:String, sx:String,yy:String)

}
