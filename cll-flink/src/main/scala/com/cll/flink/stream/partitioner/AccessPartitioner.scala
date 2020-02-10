package com.cll.flink.stream.partitioner

import org.apache.flink.api.common.functions.Partitioner

/**
 * @ClassName AssceePartitioner
 * @Description 自定义分区器
 * @Author cll
 * @Date 2020/2/10 10:02 下午
 * @Version 1.0
 **/
class AccessPartitioner extends Partitioner[String]{

  override def partition(key: String, numPartitions: Int): Int = {

    if(key == "meituan.com"){
      0
    }else if(key == "jd.com"){
      1
    }else{
      2
    }
  }

}
