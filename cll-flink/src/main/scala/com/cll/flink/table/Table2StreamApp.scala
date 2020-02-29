package com.cll.flink.table

import com.cll.flink.bean.Domain.WC
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * @ClassName Table2StreamApp
 * @Description TODO
 * @Author cll
 * @Date 2020/2/29 2:50 下午
 * @Version 1.0
 **/
object Table2StreamApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val stream = env.socketTextStream("hadoop000",9999)

    val stream2 = stream.flatMap(_.split(",")).map(a => WC(a,1))//.keyBy(_._1).sum(1)

    val table = tableEnv.fromDataStream(stream2)
    tableEnv.registerTable("wc", table)

    val resultTable = tableEnv.sqlQuery("select word,count(cnt) cnts from wc group by word")

    /*
     * 1.toAppendStream 仅仅只是追加的方式 但是流式数据状态、值什么的计算后面数据进来肯定要改动的  所以不适用
     *
     * 2.toRetractStream 这个是插入 撤回的方式 也就是先把当前数据插入 如果后面数据流进来 对应的有改动 先把已存在的数据撤回 然后再插入
     * 比如 在 nc -lk 9999 输入 a,a,a,a 字符串 打印结果如下
     * 3> (true,a,1)   插入最新数据
     * 3> (false,a,1)  撤回
     * 3> (true,a,2)   插入最新数据
     * 3> (false,a,2)  撤回
     * 3> (true,a,3)   插入最新数据
     * 3> (false,a,3)  ......
     * 3> (true,a,4)
     *
     * 3.Upsert stream
     * requires a (possibly composite) unique key.
     * The main difference to a retract stream is that UPDATE changes are encoded with a single message and hence more efficient
     * 与toRetractStream 不同之处就是 将更新操作放入一条操作  也就是只需要操作一次  因此更高效
     */
//    tableEnv.toAppendStream[Row](resultTable).print()
//    tableEnv.toRetractStream[Row](resultTable).print()

    env.execute(this.getClass.getSimpleName)
  }

}
