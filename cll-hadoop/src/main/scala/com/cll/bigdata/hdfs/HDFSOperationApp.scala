package com.cll.bigdata.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * @ClassName HDFSOperationApp
  * @Description 操作HDFS类
  * @Author cll
  * @Date 2019-12-10 15:22
  * @Version 1.0
  **/
object HDFSOperationApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("","hadoop")
    val conf = new Configuration()

    FileSystem
  }

}
