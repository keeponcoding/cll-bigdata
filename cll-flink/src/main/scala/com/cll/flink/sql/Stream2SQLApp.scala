package com.cll.flink.sql

import com.cll.flink.bean.Domain.Access
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

/**
 * @ClassName Stream2SQLApp
 * @Description TODO
 * @Author cll
 * @Date 2020/2/29 10:41 上午
 * @Version 1.0
 **/
object Stream2SQLApp {

  def main(args: Array[String]): Unit = {
    // Stream 上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2) // 设置并行度

    // table 上下文环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 读取本地文件
    val srcDS: DataStream[String] = env.readTextFile("cll-flink/data/access.log")

    // map算子处理读取的文本数据 按照指定分隔符进行切分
    val valueDS = srcDS.map(a => {
      val b = a.split(",")
      Access(b(0).trim.toLong, b(1).trim, b(2).trim.toInt)
    })

    // 从 DataStream 生成 Table
    val table: Table = tableEnv.fromDataStream(valueDS)
    // 注册临时表
    tableEnv.registerTable("access", table)

    // 执行sql
    val resultTable: Table = tableEnv.sqlQuery("select site,sum(1) num from access group by site")

    /*
     * 如果要进行 group by 操作 且 toAppendStream 则会包以下错
     * Exception in thread "main" org.apache.flink.table.api.ValidationException:
     * Table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages.
     */

    // Table 转成 DataStream   注意 toAppendStream[Row] 后面的范型
    val rowDS: DataStream[Row] = tableEnv.toAppendStream[Row](resultTable)
    rowDS.print()

    env.execute(this.getClass.getSimpleName)
  }

}
