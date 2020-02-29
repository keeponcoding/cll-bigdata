package com.cll.flink.table

import com.cll.flink.bean.Domain.Access
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

/**
 * @ClassName Batch2TableApp
 * @Description TODO
 * @Author cll
 * @Date 2020/2/29 2:26 下午
 * @Version 1.0
 **/
object Batch2TableApp {

  def main(args: Array[String]): Unit = {
    // batch 上下文环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // batch table 上下文环境
    val tableEnv = BatchTableEnvironment.create(env)

    val textDS = env.readTextFile("cll-flink/data/access.log")

    val mapDS = textDS.map(a => {
      val b = a.split(",")
      Access(b(0).trim.toLong, b(1).trim, b(2).trim.toInt)
    })

    // DataSet to Table
    val table: Table = tableEnv.fromDataSet(mapDS)

    // 直接操作able
    val resultTable = table.select("site")

    tableEnv.toDataSet[Row](resultTable).print()

  }

}
