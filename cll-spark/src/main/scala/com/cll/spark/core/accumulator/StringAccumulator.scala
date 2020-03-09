package com.cll.spark.core.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
 * @ClassName StringAccumulator
 * @Description 自定义累加器
 * @Author cll
 * @Date 2020/3/9 2:38 下午
 * @Version 1.0
 **/
class StringAccumulator extends AccumulatorV2[String, ArrayBuffer[String]]{

  // 定义存储累加器结果的容器
  private var result = ArrayBuffer[String]()

  // 判断累加器当前是否有内容
  override def isZero: Boolean = {
    this.result.size == 0
  }

  // 新建累加器 并把之前的累加器里面的值赋值给新的累加器中
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newStrAccu = new StringAccumulator
    newStrAccu.result = this.result
    newStrAccu
  }

  // 重置累加器
  override def reset(): Unit = {
    this.result = new ArrayBuffer[String]()
  }

  // 累加器添加值
  override def add(v: String): Unit = {
    this.result.append(v)
  }

  // 把两个累加器的值合并起来
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    this.result.++=:(other.value)
  }

  override def value: ArrayBuffer[String] = {
    this.result
  }
}
