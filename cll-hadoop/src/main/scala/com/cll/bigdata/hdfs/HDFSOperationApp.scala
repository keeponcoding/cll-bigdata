package com.cll.bigdata.hdfs

import java.net.URI

import com.cll.bigdata.hdfs.FileRenameApp.{HDFA_PATH, conf, fileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * @ClassName HDFSOperationApp
  * @Description 操作HDFS类
  *             常用hdfs api
  * @Author cll
  * @Date 2019-12-10 15:22
  * @Version 1.0
  **/
object HDFSOperationApp {

  val HDFA_PATH = "hdfs://hadoop000:9000"

  var conf:Configuration = _

  var fileSystem:FileSystem = _

  def main(args: Array[String]): Unit = {
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

    // 从本地拷贝文件到hdfs
    fileSystem.copyFromLocalFile(new Path("src"), new Path("dst"))

    //
    fileSystem.moveFromLocalFile(new Path("src"), new Path("dst"))
    fileSystem.moveFromLocalFile(Array(new Path("src")), new Path("dst"))

    // 从hdfs拷贝文件到本地
    fileSystem.copyToLocalFile(new Path("src"), new Path("dst"))

    fileSystem.moveToLocalFile(new Path("src"), new Path("dst"))

    // 删除指定路径  是否递归删除
    fileSystem.delete(new Path("dir"), false)

    // 更改名字
    fileSystem.rename(new Path("src"), new Path("dst"))

    // 判断指定路径是否存在
    fileSystem.exists(new Path("dir"))

    // 判断指定路径是文件夹还是文件
    fileSystem.isDirectory(new Path("dir"))
    fileSystem.isFile(new Path("dir"))

    // 创建指定路径
    fileSystem.mkdirs(new Path("dir"))

    // 关闭资源
    fileSystem.close()
  }

}
