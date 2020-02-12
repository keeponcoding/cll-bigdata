package com.cll.flink.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.api.scala._

/**
 * @ClassName Kafka2KafkaApp
 * @Description 接受kafka数据 处理之后 再发送到kafka
 * @Author cll
 * @Date 2020/2/12 10:46 上午
 * @Version 1.0
 **/
object Kafka2KafkaApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
     * TODO kafka source
     */
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop000:9092")
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "cll-flink-test")

    val consumer = new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()      // start from the earliest record possible
    // consumer.setStartFromLatest()        // start from the latest record
    // consumer.setStartFromTimestamp(System.currentTimeMillis())  // start from specified epoch timestamp (milliseconds)
    // consumer.setStartFromGroupOffsets()  // the default behaviour

    val stream = env.addSource(consumer)
    // val value = stream.map((_, 1)).keyBy(0).sum(1)

    /*
     * TODO kafka sink
     */
    val producer = new FlinkKafkaProducer010[String](
      "hadoop000:9092",
      "test2",
      new SimpleStringSchema
    )
    // to kafka
    stream.addSink(producer)

    env.execute(this.getClass.getSimpleName)
  }

}
