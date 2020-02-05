package com.cll.spark.ss

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * @ClassName StreamingKafkaApp
 * @Description 该offset维护在kafka自身
 *             默认的topic是 __consumer_offsets
 *             TODO 可通过以下命令查询自身维护的offset
 *             bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
 *             --broker-list ... \
 *             --topic test
 * @Author cll
 * @Date 2020/2/4 4:35 下午
 * @Version 1.0
 **/
object StreamingKafkaApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop000:9092,hadoop000:9093,hadoop000:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest", //earliest latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // stream.map(_.value()).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()

    // 注意 rdd 需要为kafka的rdd
    stream.foreachRDD { rdd =>

      println("~~~~~~~~~~~:" + rdd.partitions.size)

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

      // some time later, after outputs have completed
      // 提交offset
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
