package com.cll.flink.util

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.util.Random

/**
 * @ClassName GeneratorLog
 * @Description TODO
 * @Author cll
 * @Date 2020/2/13 9:53 上午
 * @Version 1.0
 **/
object GeneratorLog {

  val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val props = new Properties();
    props.put("bootstrap.servers", "hadoop000:9092,hadoop000:9093,hadoop000:9094");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    val producer = new KafkaProducer[String,String](props);

    val domains = Array("meituan.com","alibaba.com","jd.com")
    val random = new Random()

    for (i <- 0 to 10) {
      Thread.sleep(100)

      val result = new StringBuffer().append("cll").append("\t")
          .append("CN").append("\t")
          .append("T").append("\t")
          .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)).append("\t")
          .append(domains(random.nextInt(domains.length))).append("\t")
          .append(random.nextInt(10000)).toString

      producer.send(new ProducerRecord[String, String]("flink-join", null, result))
    }

    producer.close();
  }

}
