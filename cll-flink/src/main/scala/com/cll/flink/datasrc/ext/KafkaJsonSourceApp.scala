package com.cll.flink.datasrc.ext

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

/**
 * @ClassName KafkaJsonSourceApp
 * @Description kafka json 数据源
 * @Author cll
 * @Date 2020/2/29 3:23 下午
 * @Version 1.0
 **/
object KafkaJsonSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val kafkaDS = tableEnv.connect(
      new Kafka()
        .version("0.10")    // required: valid connector versions are
        //   "0.8", "0.9", "0.10", "0.11", and "universal"
        .topic("test")       // required: topic name from which the table is read

        // optional: connector specific properties
        .property("zookeeper.connect", "hadoop000:2181")
        .property("bootstrap.servers", "hadoop000:9092")
        .property("group.id", "cll-flink-test")

        // optional: select a startup mode for Kafka offsets
//        .startFromEarliest()
        .startFromLatest()
//        .startFromSpecificOffsets(...)

        // optional: output partitioning from Flink's partitions into Kafka's partitions
//        .sinkPartitionerFixed()         // each Flink partition ends up in at-most one Kafka partition (default)
        .sinkPartitionerRoundRobin()    // a Flink partition is distributed to Kafka partitions round-robin
//        .sinkPartitionerCustom(MyCustom.class)    // use a custom FlinkKafkaPartitioner subclass
    )
    .withFormat(                                  // required: Kafka connector requires to specify a format,
                                                 // the supported formats are Csv, Json and Avro.
      new Json()
        .failOnMissingField(false)
        .deriveSchema()
    ).withSchema(
      new Schema()
      .field("userId", Types.LONG)
      .field("day", Types.STRING)
      .field("data", Types.STRING)
    ).inAppendMode()
      .registerTableSource("kafka_access")

    // 测试数据
    // {"userId":123456, "day":"2020022915", "data":"hello world1"}
    // {"userId":234567, "day":"2020022916", "data":"hello world2"}

    val resultTable = tableEnv.sqlQuery("select * from kafka_access")
    tableEnv.toAppendStream[Row](resultTable).print()

    env.execute(this.getClass.getSimpleName)
  }

}
