package com.cll.flink.sql

import com.cll.flink.bean.Domain.Access
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

/**
 * @ClassName Batch2SQLApp
 * @Description TODO
 * @Author cll
 * @Date 2020/2/29 11:26 上午
 * @Version 1.0
 **/
object Batch2SQLApp {

  def main(args: Array[String]): Unit = {
    // 批处理上下文环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // table 上下文环境
    val tableEnv = BatchTableEnvironment.create(env)

    val textDS = env.readTextFile("cll-flink/data/access.log")

    val mapDS = textDS.map(a => {
      val b = a.split(",")
      Access(b(0).trim.toLong, b(1).trim, b(2).trim.toInt)
    })

    // DataSet 转 Table
    val table: Table = tableEnv.fromDataSet(mapDS)
    // 注册临时表
    tableEnv.registerTable("access", table)

    // Table 转 DataSet
    val resultTable: Table = tableEnv.sqlQuery("select site,count(1) num from access group by site")
    resultTable.printSchema()
    val value = tableEnv.toDataSet[Row](resultTable)
    value.print()
  }

}
