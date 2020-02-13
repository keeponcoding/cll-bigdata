package com.cll.flink.stream.joincase

import java.sql.{Connection, PreparedStatement}

import com.cll.flink.util.MySQLUtil
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.mutable

/**
 * @ClassName UserDomainMySQLSource
 * @Description 读取 user_domain 表数据
 * @Author cll
 * @Date 2020/2/13 9:59 上午
 * @Version 1.0
 **/
class UserDomainMySQLSource extends SourceFunction[(String, String)]{

  var running = true

  var conn:Connection = _
  var pstat:PreparedStatement = _

  override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
    conn = MySQLUtil.getConnection()
    pstat = conn.prepareStatement("select id,domains from user_domain")
    val rs = pstat.executeQuery()

    while (rs.next()){
      val id = rs.getString("id")
      val domain = rs.getString("domains")
      ctx.collect((domain,id))
    }

  }

  override def cancel(): Unit = {
    running = false
  }
}
