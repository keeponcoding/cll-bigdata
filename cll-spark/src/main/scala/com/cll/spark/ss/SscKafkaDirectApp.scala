package com.cll.spark.ss

import com.cll.spark.ss.offset.MysqlOffsetManager
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @ClassName SscKafkaDirectApp
  * @Description TODO
  * @Author cll
  * @Date 2020-01-08 22:26
  * @Version 1.0
  **/
object SscKafkaDirectApp {

  val TOPICS = Array("hadoop-offset")
  val GROUPID = "hadoop-kafka-offset-groupid"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop000:9092,hadoop000:9093,hadoop000:9094",// 这里不需要写所有的kafka
      "key.deserializer" -> classOf[StringDeserializer], // 跟生产者产生数据的序列化保持一致
      "value.deserializer" -> classOf[StringDeserializer], // 跟生产者产生数据的序列化保持一致
      "group.id" -> GROUPID, // 消费组
      "auto.offset.reset" -> "earliest", // latest 最近的  earliest  最早的
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // TODO 该处需要解决 SscKafkaDirectApp3 中结尾的问题 就是去获取已经提交的offset的记录信息
    //    val fromOffsets = RedisOffsetManager.obtainOffsets(TOPICS(0), GROUPID)
    val fromOffsets = MysqlOffsetManager.obtainOffsets(TOPICS(0), GROUPID)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, // 策略  数据尽量均匀的分布到各个executor上去
      Subscribe[String, String](TOPICS, kafkaParams, fromOffsets) // 订阅 TODO 注意这里多了个fromOffsets
    )

    //    val result = stream.map(_.value()).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
    //    result.print()

    // 入库
    stream.foreachRDD(rdd => {
      // 判断rdd中是否有数据
      if(!rdd.isEmpty()){
        println("~~~~~~~~~~~~~~~~~~~分区数:"+rdd.partitions.size )

        /*
         * TODO 获取当前批次的offset
         * 使用result.foreachRDD   rdd.partitions.size == 2
         * 使用stream.foreachRDD   rdd.partitions.size == 3
         *
         * 如果使用result.foreachRDD  那么rdd就是一个普通的rdd   不能 进行 rdd.asInstanceOf[HasOffsetRanges]
         * 直接用stream.foreachRDD 可以进行 rdd.asInstanceOf[HasOffsetRanges]
         */
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(x => {
          println(s"${x.topic} ${x.partition} ${x.fromOffset} ${x.untilOffset}")
        })

        /*
         * TODO 业务逻辑处理
         */

        /*
         * TODO 提交 offset 到 redis
         */
        MysqlOffsetManager.storeOffsets(offsetRanges, GROUPID)

      } else {
        println("当前批次没有数据流入......")
      }
    })

    ssc.start() // start 过后 不再进行业务逻辑处理
    ssc.awaitTermination()
  }

}
