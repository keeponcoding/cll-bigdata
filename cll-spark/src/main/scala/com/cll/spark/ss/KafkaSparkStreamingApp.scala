package com.cll.spark.ss

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ClassName KafkaSparkStreamingApp
  * @Description TODO
  * @Author cll
  * @Date 2019-12-10 10:33
  * @Version 1.0
  **/
object KafkaSparkStreamingApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")
    /*val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value))*/

    ssc.start() // 启动
    ssc.awaitTermination()
  }

}
