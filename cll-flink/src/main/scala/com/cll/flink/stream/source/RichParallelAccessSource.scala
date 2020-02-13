package com.cll.flink.stream.source

import java.util.Random

import com.cll.flink.bean.Domain.Access
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @ClassName AccessSource
 * @Description 自定义Source   ParallelSourceFunction
 * @Author cll
 * @Date 2020/2/10 3:25 下午
 * @Version 1.0
 **/
class RichParallelAccessSource extends RichParallelSourceFunction[Access]{

  var running = true

  override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
    val domains = Array("jd.com","baidu.com","meituan.com")

    val random = new Random()

    while (running) {
      val timestamp = System.currentTimeMillis()
      1.to(10).map(a => {
        ctx.collect(Access(timestamp, domains(random.nextInt(domains.length)), random.nextInt(10000) + a))
      })

      Thread.sleep(5000)

    }

  }

  override def cancel(): Unit = {
    running = false
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }
}
