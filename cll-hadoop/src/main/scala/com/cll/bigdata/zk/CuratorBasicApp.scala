package com.cll.bigdata.zk

import java.util

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat

/**
  * @ClassName CuratorApp
  * @Description 操作zk
  * @Author cll
  * @Date 2019-12-11 19:06
  * @Version 1.0
  **/
object CuratorBasicApp {

  def main(args: Array[String]): Unit = {
    val retry = new ExponentialBackoffRetry(1000, 3)

    /*
     * String connectString 服务器列表
     * int sessionTimeoutMs 会话超时时间
     * int connectionTimeoutMs 连接创建超时时间
     * RetryPolicy retryPolicy 重试策略 内建四中重试策略 也可以自定义 实现 RetryPolicy 即可
     */
    val connectString = "hadoop000:2181"
    val sessionTimeoutMs = 5000
    val connectionTimeoutMs = 5000

    // 使用静态工厂方式创建客户端
    // val client = CuratorFrameworkFactory.newClient(connectString,sessionTimeoutMs,connectionTimeoutMs,retry)

    // 使用 fluent 风格创建客户端
    val client = CuratorFrameworkFactory.builder()
                   .connectString(connectString)
                   .sessionTimeoutMs(sessionTimeoutMs)
                   .connectionTimeoutMs(connectionTimeoutMs)
                   .retryPolicy(retry)
                   .namespace("test") // Chroot 客户端隔离 设置该参数过后 之后该client所有的操作都只能在 test 下进行
                   .build()

    // 启动客户端
    client.start()

    /*
     * TODO 创建节点
     */
    // 创建指定node 不设置节点属性 默认为 持久化节点
    // client.create().forPath("/test")

    // 创建指定node 并初始化数据
    // client.create().forPath("/test","test".getBytes())

    /*
     * 创建节点指定模式 设置节点属性
     *
     * PERSISTENT              持久化
     * PERSISTENT_SEQUENTIAL   持久化
     * EPHEMERAL               临时
     * EPHEMERAL_SEQUENTIAL    临时
     */
    /*client.create()
      //.creatingParentsIfNeeded() 自动递归创建父节点 不包含 container  就是创建一个普通的持久化父节点
      .creatingParentContainersIfNeeded() // 自动递归创建父节点 节省了对父节点是否存在的判断 包含container  该特性是zk新版本才支持的
      .withMode(CreateMode.EPHEMERAL)
      .forPath("/test")*/

    /*
     * TODO 删除节点
     */
    // 删除指定节点  该方法只能删除子节点 否则会报错
    // client.delete().forPath("/test")

    // 复杂删除节点
    /*client.delete()
      .guaranteed()                // 强制保证删除 只要客户端回话有效 就会在后台持续进行删除 直到删除成功
      .deletingChildrenIfNeeded()  // 删除节点 并且递归删除该节点下的所有子节点
      .withVersion(1)     // 指定版本进行删除
      .forPath("/test")*/

    /*
     * TODO 读取
     */
    // 读取节点内容 返回的是 byte[]
    /*val data = client.getData.forPath("/")
    println(new String(data))*/

    // 读取节点数据 并且获取到该节点的stat
    /*val stat = new Stat()
    val bytes = client.getData
      .storingStatIn(stat) // 保存该节点的stat信息
      .forPath("/")
    println("data:"+new String(bytes)+"\n"
      +"version:"+stat.getVersion+"\n"
      +"Ctime:"+stat.getCtime+"\n"
      +"Cversion:"+stat.getCversion+"\n"
      +"Czxid:"+stat.getCzxid+"\n"
      +"DataLength:"+stat.getDataLength+"\n"
      +"EphemeralOwner:"+stat.getEphemeralOwner+"\n"
      +"Mtime:"+stat.getMtime+"\n"
      +"Mzxid:"+stat.getMzxid+"\n"
      +"NumChildren:"+stat.getNumChildren+"\n"
      +"Pzxid:"+stat.getPzxid+"\n"
      +"Aversion:"+stat.getAversion+"\n"
    )*/

    /*
     * TODO 更新节点信息
     */
    /*client.setData()
      .withVersion(0) // 指定版本进行更新
      .forPath("/", "test1".getBytes())*/

    /*
     * TODO 判断节点是否存在
     */
    /*val stat = client.checkExists().forPath("/")
    println(stat == null)*/

    /*
     * TODO 获取该节点下的所有子节点路径
     */
    val childPath = client.getChildren.forPath("/")
    println(childPath)

    // 关闭资源
    client.close()
  }

}
