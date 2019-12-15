package com.cll.hadoop.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

/**
  * @ClassName FileRenameApp
  * @Description 更改文件名
  *  需求：将指定目录文件更改名称 如下
  *  原文件：
  *  /log/20191001/a
  *  /log/20191001/b
  *  /log/20191001/c
  *
  *  更名为：
  *  /log/2019/10/01/a-20191001
  *  /log/2019/10/01/b-20191001
  *  /log/2019/10/01/c-20191001
  * @Author cll
  * @Date 2019-12-11 14:40
  * @Version 1.0
  **/
object FileRenameApp {

  val HDFA_PATH = "hdfs://hadoop000:9000"

  var conf:Configuration = _

  var fileSystem:FileSystem = _

  def main(args: Array[String]): Unit = {

    val path = "/log/20191001" //args(0)

    conf = new Configuration()
    // 阿里云 开启hostname访问
    conf.set("dfs.client.use.datanode.hostname", "true")
    // 设置副本个数
    conf.set("dfs.replication", "1")

    /*
     * HDFS_PATH
     * conf 配置信息
     * user 以什么用户去访问
     */
    fileSystem = FileSystem.get(new URI(HDFA_PATH), conf, "hadoop")

    rename(path)

    // 关闭资源
    fileSystem.close()
  }

  /**
    * 更改hdfs文件名操作
    * @param path
    */
  def rename(path:String) = {

    /*
     * path  路径
     * recursive 是否递归
     */
    val files = fileSystem.listFiles(new Path(path), false)

    while (files.hasNext) {
      val file = files.next()
      // hdfs://hadoop000:9000/log/20191001/a.txt
      val path = file.getPath
      val pathArr = path.toString.split("/")
      val len = pathArr.length

      // 日期目录名称
      val dateDir = pathArr(len-2)
      val year = dateDir.substring(0,4)
      val month = dateDir.substring(4,6)
      val day = dateDir.substring(6)
      val dst = HDFA_PATH + "/log/" + Seq(year,month,day).mkString("/") + "/"

      // 判断新路径是否存在
      if(!fileSystem.exists(new Path(dst))){
        fileSystem.mkdirs(new Path(dst))
      }

      // 文件名 a
      val srcName = path.getName
      val newName = srcName + "-" + dateDir

      // src dst
      fileSystem.rename(path, new Path(dst+newName))
    }

  }

}
