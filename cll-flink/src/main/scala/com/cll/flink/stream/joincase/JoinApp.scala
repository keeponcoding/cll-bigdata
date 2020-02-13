package com.cll.flink.stream.joincase

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @ClassName JoinApp
 * @Description 需求：从MySQL查询出数据 与 从kafka接受的日志信息进行关联操作
 * @Author cll
 * @Date 2020/2/13 10:04 上午
 * @Version 1.0
 **/
object JoinApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
     * Kafka source
     */
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop000:9092")
    properties.setProperty("group.id", "cll-flink-join-log")

    val consumer = new FlinkKafkaConsumer010[String]("flink-join", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()

    val logStream = env.addSource(consumer).filter(_.length > 1).map(x => {
      val a = x.split("\t")
      (a(4).trim,a(5).toInt,x)
    })
    println("logStream 并行度 : " + logStream.parallelism)

    /*
     * MySQL source
     */
    val mysqlStream = env.addSource(new UserDomainMySQLSource).broadcast
    println("mysqlStream 并行度 : " + mysqlStream.parallelism)

    // connect 操作
    logStream.connect(mysqlStream).flatMap(new CoFlatMapFunction[(String,Int,String),(String,String),String] {

      var data = new mutable.HashMap[String,String]()

      // logStream
      override def flatMap1(value: (String, Int, String), out: Collector[String]): Unit = {
        val domain = value._1
        val id = data.getOrElse(domain,"-99")
        out.collect(domain +" ---> "+ id)
      }

      // mysqlStream
      override def flatMap2(value: (String, String), out: Collector[String]): Unit = {
        val domain = value._1 // domain
        val id = value._2 // id
        data.put(domain,id)
      }
    }).print()

    /*
     * TODO 从输出结果来看 有匹配到的 还有匹配不到的 打印出 -99
     *
     * 打印结果：
     *  8> meituan.com ---> -99
     *  7> alibaba.com ---> 222222
     *  1> jd.com ---> -99
     *  8> jd.com ---> 333333
     *  7> meituan.com ---> -99
     *  1> jd.com ---> -99
     *  8> meituan.com ---> -99
     *  7> jd.com ---> -99
     *  1> jd.com ---> -99
     *  8> alibaba.com ---> -99
     *  7> meituan.com ---> -99
     *
     *  原因分析：mysqlsource 继承的是 SourceFunction 它的并行度只能是 1
     *          logstream 数据来自于kafka 并行度是默认的机器核数 8  (也可以程序设置 env.setParallelism(3) )
     *          这样两个stream关联起来就会有问题
     *  解决方案：
     *     val mysqlStream = env.addSource(new MySQLSource) 更改为 val mysqlStream = env.addSource(new MySQLSource).broadcast
     *     ★ broadcast --> Sets the partitioning of the DataStream
     *                     so that the output tuples are broad casted to every parallel instance of the next component.
     *
     * 打印结果：
     * 关闭JoinApp 重启运行的结果(运行 第一次打印结果)
     * 2> alibaba.com ---> -99
     * 3> alibaba.com ---> -99
     * 4> jd.com ---> -99
     * 2> meituan.com ---> -99
     * 3> alibaba.com ---> -99
     * 2> jd.com ---> -99
     * 4> meituan.com ---> -99
     * 3> meituan.com ---> -99
     * 2> alibaba.com ---> -99
     * 3> alibaba.com ---> -99
     * 4> jd.com ---> -99
     *
     * JoinApp开启的状态 再生成一次log日志(GeneratorLog) 会发现打印结果 都匹配上了
     * 4> jd.com ---> 333333
     * 3> alibaba.com ---> 222222
     * 2> meituan.com ---> 111111
     * 4> alibaba.com ---> 222222
     * 3> jd.com ---> 333333
     * 2> alibaba.com ---> 222222
     * 4> meituan.com ---> 111111
     * 3> meituan.com ---> 111111
     * 2> jd.com ---> 333333
     * 4> jd.com ---> 333333
     * 3> jd.com ---> 333333
     *
     * 反复测试，每次重新云心JoinApp 第一次打印结果都是匹配不上的情况  再次生成log日志 又可以匹配上
     */

    env.execute(this.getClass.getSimpleName)
  }

}
