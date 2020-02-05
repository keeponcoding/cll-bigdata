package com.cll.spark.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * @ClassName GenerateData
  * @Description 生产数据
  * @Author cll
  * @Date 2019-12-10 14:40
  * @Version 1.0
  **/
object GenerateData {

  val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    val props = new Properties();
    props.put("bootstrap.servers", "hadoop000:9092,hadoop000:9093,hadoop000:9094");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    val producer = new KafkaProducer[String,String](props);

    for (i <- 0 to 10) {
      Thread.sleep(100)

      val word = String.valueOf((new Random().nextInt(6) + 'a').toChar)
      val part = i % 3

      log.error("生成数据：" + word)

      producer.send(new ProducerRecord[String, String]("test", null, word))
    }

    producer.close();

  }

}
