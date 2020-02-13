package com.cll.flink.stream

import com.cll.flink.bean.Domain.User
import com.cll.flink.stream.sink.MySQLSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ClassName Result2MySQLApp
 * @Description 处理结果插入到mysql
 * @Author cll
 * @Date 2020/2/12 11:15 上午
 * @Version 1.0
 **/
object Result2MySQLApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("cll-flink/data/user.data")
    val stream = text.map(x => {
      val a = x.split(",")
      User(a(0).toInt, a(1), a(2).toInt)
    })

    stream.addSink(MySQLSink)

    // Caused by: org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException:
    // Could not forward element to next operator
    // Caused by: java.sql.SQLException: No operations allowed after statement closed.
    // 解决方案：statement 开启
    env.execute(this.getClass.getSimpleName)
  }

}
