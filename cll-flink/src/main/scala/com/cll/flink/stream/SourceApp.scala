package com.cll.flink.stream

import java.util.Properties

import com.cll.flink.stream.custom.MySQLSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
 * @ClassName SourceApp
 * @Description 各种 数据源
 * @Author cll
 * @Date 2020/2/10 3:16 下午
 * @Version 1.0
 **/
object SourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    env.fromCollection(List("java,scala","hello,datasource")).print()

//    env.fromElements(1,2,3,4,5).print()

    /*
     * TODO 添加自定义MySQL Source
     */
//    env.addSource(new MySQLSource).print()

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

    env.addSource(consumer).print()


    env.execute(this.getClass.getSimpleName)
  }

}
