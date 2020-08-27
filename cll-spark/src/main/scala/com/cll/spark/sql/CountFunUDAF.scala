package com.cll.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

/**
 * @ClassName CountFunUDAF
 * @Description 自定义 UDAF 函数  实现  count 功能
 * @Author cll
 * @Date 2020/8/27 3:50 下午
 * @Version 1.0
 **/
object CountFunUDAF extends UserDefinedAggregateFunction{

  // 指定输入数据的类型
  override def inputSchema: StructType = {
    // StructField 第三个参数 设定传入的参数是否可为 null
    StructType(Array(StructField("str", StringType, true)))
  }

  // 中间聚合时所处理的数据类型
  override def bufferSchema: StructType = {
    // StructField 第三个参数 设定传入的参数是否可为 null
    StructType(Array(StructField("cnt", IntegerType, true)))
  }

  // 返回的数据类型
  override def dataType: DataType = {
    IntegerType
  }

  override def deterministic: Boolean = true

  // 初始化 分组数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  // 节点上的局部聚合
  // 每个分组新进来的值的具体的操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  // 由于spark是分布式的。所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  // 但是，最后一个分组会在节点上聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  // 返回最终结果值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
