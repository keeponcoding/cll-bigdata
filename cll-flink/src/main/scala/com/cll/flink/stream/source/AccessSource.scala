package com.cll.flink.stream.source

import java.util.Random

import com.cll.flink.bean.Domain.Access
import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @ClassName AccessSource
 * @Description 自定义Source
 * @Author cll
 * @Date 2020/2/10 3:25 下午
 * @Version 1.0
 **/
class AccessSource extends SourceFunction[Access]{

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
}
