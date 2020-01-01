package com.cll.spark.ss

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @ClassName KafkaSparkStreamingApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-10 10:33
  * @Version 1.0
  **/
object KafkaSparkStreamingApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop000:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "maxwell-kafka",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("maxwell")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    stream.map(record => (record.key, record.value)).print()

    ssc.start() // 启动
    ssc.awaitTermination()
  }

}
