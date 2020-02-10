package com.cll.flink.stream

import com.cll.flink.bean.Domain.Access
import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * @ClassName SpecTransforFuncApp
 * @Description TODO
 * @Author cll
 * @Date 2020/2/10 2:33 下午
 * @Version 1.0
 **/
object SpecTransforFuncApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度 全局
    env.setParallelism(1)

    // 读取文本数据
    val text: DataStream[String] = env.readTextFile("cll-flink/data/access.log")

    /*val value = text.map(x => {
      val a = x.split(",")
      Access(a(0).toLong, a(1), a(2).toInt)
    })*/
    // 改写map
    val value = text.map(new CLLRichMap)

    /*
     * TODO 需求：过滤 traffic > 2000
     */
    // ① filter
    value.filter(_.traffic > 2000 ).print()

    // ② 自定义Filter   继承FilterFunction
    // value.filter(new CLLFilter(2000)).print()
    // 同② 使用匿名内部类
    /*value.filter(new FilterFunction[Access] {
      override def filter(value: Access): Boolean = value.traffic > 2000
    }).print()*/

    // ③ RichFilterFunction
    /*value.filter(new RichFilterFunction[Access] {
      override def filter(value: Access): Boolean = value.traffic > 2000
    }).print()*/

    env.execute(this.getClass.getSimpleName)
  }
}

class CLLFilter(filterValue:Int) extends FilterFunction[Access]{
  override def filter(value: Access): Boolean = value.traffic > filterValue
}

// 自定义RichMap
class CLLRichMap extends RichMapFunction[String, Access] {
  override def map(value: String): Access = {
    val a = value.split(",")
    Access(a(0).toLong, a(1), a(2).toInt)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    println(">>>>>>>>>>>open>>>>>>>>>>>")
  }

  override def close(): Unit = {
    super.close()
    println(">>>>>>>>>>>close>>>>>>>>>>>")
  }

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext
}
