package com.cll.flink.stream.custom

import java.sql.{Connection, PreparedStatement}

import com.cll.flink.bean.Domain.Student
import com.cll.flink.util.MySQLUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
 * @ClassName MySQLSource
 * @Description MySQL数据源
 * @Author cll
 * @Date 2020/2/10 7:58 下午
 * @Version 1.0
 **/
class MySQLSource extends RichSourceFunction[Student]{

  var conn:Connection = _
  var pstmt:PreparedStatement = _

  // 获取连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = MySQLUtil.getConnection()
    pstmt = conn.prepareStatement("select * from student")
  }

  // 释放连接
  override def close(): Unit = {
    super.close()
    MySQLUtil.close(conn,pstmt)
  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    val rs = pstmt.executeQuery()
    while (rs.next()){
      val student = Student(rs.getInt("id"), rs.getString("name"), rs.getInt("age"))
      ctx.collect(student)
    }
  }

  override def cancel(): Unit = {

  }
}
