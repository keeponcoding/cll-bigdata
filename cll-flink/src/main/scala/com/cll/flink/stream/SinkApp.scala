package com.cll.flink.stream

import com.cll.flink.bean.Domain.Access
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
/**
 * @ClassName SinkApp
 * @Description 写出
 * @Author cll
 * @Date 2020/2/12 10:37 上午
 * @Version 1.0
 **/
object SinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //
    val stream = env.readTextFile("cll-flink/data/access.log")
      .map(x => {
        val a = x.split(",")
        Access(a(0).toLong, a(1), a(2).toInt).toString
      })

    val producer = new FlinkKafkaProducer010[String](
      "hadoop000:9092",
      "test",
      new SimpleStringSchema
    )

    // to kafka
    stream.addSink(producer)

    // to local
    stream.print()

    env.execute(this.getClass.getSimpleName)
  }

}
