package com.cll.flink.stream.sink

import java.sql.{Connection, PreparedStatement}

import com.cll.flink.bean.Domain.User
import com.cll.flink.util.MySQLUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @ClassName MySQLSink
 * @Description flink官网没有自带mysql sink
 *             自定义MySQL sink
 * @Author cll
 * @Date 2020/2/12 10:59 上午
 * @Version 1.0
 **/
object MySQLSink extends RichSinkFunction[User]{

  var conn:Connection = _
  var insertPstmt:PreparedStatement = _
  var updatePstmt:PreparedStatement = _

  /*
   * 获取连接
   */
  override def open(parameters: Configuration): Unit = {
    conn = MySQLUtil.getConnection()
    insertPstmt = conn.prepareStatement("insert into t_user(id,name,order_no) value(?,?,?)")
    updatePstmt = conn.prepareStatement("update t_user set name = ? where order_no = ?")
  }

  /*
   * 释放资源
   */
  /*override def close(): Unit = {
    if(insertPstmt != null) insertPstmt.close()
    if(updatePstmt != null) updatePstmt.close()
    if(conn != null) conn.close()
  }*/

  /*
   * 具体业务逻辑  写数据
   */
  override def invoke(user: User, context: SinkFunction.Context[_]): Unit = {

    updatePstmt.setString(1,user.name)
    updatePstmt.setString(2,user.order_no.toString)
    updatePstmt.execute()

    if(updatePstmt.getUpdateCount == 0){
      insertPstmt.setString(1,user.id.toString)
      insertPstmt.setString(2,user.name.toString)
      insertPstmt.setString(3,user.order_no.toString)
      insertPstmt.execute()
    }
  }

}
