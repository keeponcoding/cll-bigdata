package com.cll.flink.util


import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @ClassName MySQLUtil
 * @Description MySQL工具类
 * @Author cll
 * @Date 2020/2/10 8:05 下午
 * @Version 1.0
 **/
object MySQLUtil {

  val JDBC_DRIVER = "com.mysql.jdbc.Driver"
  val JDBC_URL = "jdbc:mysql://hadoop000:3306/cll"
  val JDBC_USER = "root"
  val JDBC_PASSWORD = "123456"

  def getConnection():Connection = {
    Class.forName(JDBC_DRIVER)
    DriverManager.getConnection(JDBC_URL,JDBC_USER,JDBC_PASSWORD)
  }

  def close(conn:Connection): Unit ={
    if(null != conn){
      conn.close()
    }
  }

  def close(conn:Connection, pstmt:PreparedStatement): Unit ={
    close(conn)

    if(null != pstmt){
      pstmt.close()
    }
  }

}
