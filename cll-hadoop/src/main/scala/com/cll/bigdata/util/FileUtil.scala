package com.cll.bigdata.util

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * @ClassName FileUtil
  * @Description 文件工具类
  * @Author cll
  * @Date 2019-12-11 15:02
  * @Version 1.0
  **/
object FileUtil {

  @throws[Exception]
  def deleteTarget(output: String, conf: Configuration): Unit = {
    val fileSystem = FileSystem.get(conf)
    val path = new Path(output)
    // 判断是否存在
    if (fileSystem.exists(path)) fileSystem.delete(path, true)
  }


  /**
    *
    * @param hdfsPath
    * @param output
    * @param conf
    * @throws Exception
    */
  @throws[Exception]
  def deleteTarget1(hdfsPath: String, output: String, conf: Configuration): Unit = {
    val fileSystem = FileSystem.get(new URI(hdfsPath), conf, "hadoop")
    val path = new Path(output)
    if (fileSystem.exists(path)) fileSystem.delete(path, true)
  }

}
