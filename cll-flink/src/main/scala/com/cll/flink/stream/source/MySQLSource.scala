package com.cll.flink.stream.source

import java.sql.{Connection, PreparedStatement}

import com.cll.flink.bean.Domain.User
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
class MySQLSource extends RichSourceFunction[User]{

  var conn:Connection = _
  var pstmt:PreparedStatement = _

  // 获取连接
  override def open(parameters: Configuration): Unit = {
    conn = MySQLUtil.getConnection()
    pstmt = conn.prepareStatement("select * from t_user")
  }

  // 释放连接
  override def close(): Unit = {
    MySQLUtil.close(conn,pstmt)
  }

  override def run(ctx: SourceFunction.SourceContext[User]): Unit = {
    val rs = pstmt.executeQuery()
    while (rs.next()){
      val user = User(rs.getInt("id"), rs.getString("name"), rs.getInt("order_no"))
      ctx.collect(user)
    }
  }

  override def cancel(): Unit = {

  }
}
