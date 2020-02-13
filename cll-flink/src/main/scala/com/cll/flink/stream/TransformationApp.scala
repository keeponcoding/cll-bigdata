package com.cll.flink.stream

import com.cll.flink.bean.Domain.Access
import com.cll.flink.stream.partitioner.AccessPartitioner
import com.cll.flink.stream.source.AccessSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ClassName TransformationApp
 * @Description Transformation 算子 的练习
 * @Author cll
 * @Date 2020/2/10 9:32 下午
 * @Version 1.0
 **/
object TransformationApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    /*val text = env.readTextFile("cll-flink/data/access.log")

    val value = text.map(x => {
      val a = x.split(",")
      Access(a(0).toLong, a(1), a(2).toInt)
    })*/

    /*
     * TODO 需求：相同的site 对traffic求和
     * 以下由两种实现方式
     */
    //value.keyBy("site").sum("traffic").print("sum")

    /*value.keyBy("site").reduce((a,b) => {
      Access(a.time, a.site, a.traffic+b.traffic+1)
    }).print("sum")*/

    /*
     * TODO 将流拆开 分流
     */
    /*val split = value.keyBy("site").sum("traffic").split(x => {
      if(x.traffic > 2000){
        Seq("big")
      }else{
        Seq("normal")
      }
    })

    // select 挑出指定的
    split.select("big").print("big")
    split.select("normal").print("normal")*/

    /*
     * TODO 合流
     *  union
     *  connect
     */
    /*val stream1 = env.addSource(new AccessSource)
    val stream2 = env.addSource(new AccessSource)

    // TODO ★ union 该算子的前提是 the same type with each other 也就是两个相union的stream的类型需要相同才行
    /*stream1.union(stream2).map(x =>{
      println("接收到的数据 : " + x)
      x
    }).print()*/

    // TODO ★ connect 该算子  DataStream outputs of different type with each other
    stream1.connect(stream2)*/


    /*
     * TODO 自定义分区器
     */
    val text = env.addSource(new AccessSource)
    text.map(x => {
      (x.site,x)
    }).partitionCustom(new AccessPartitioner,0) // 设置自定义的分区器
      .map(a => {
        println("当前线程id : ["+Thread.currentThread().getId+"]，【"+a._2+"】")
        a._2
      }).print()

    env.execute(this.getClass.getSimpleName)
  }

}
