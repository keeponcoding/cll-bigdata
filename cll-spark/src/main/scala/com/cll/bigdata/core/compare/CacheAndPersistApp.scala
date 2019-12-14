package com.cll.bigdata.core.compare

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName CacheRDDApp
  * @Description 缓存
  * @Author cll
  * @Date 2019-12-14 15:25
  * @Version 1.0
  **/
object CacheAndPersistApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // cache 之后 可以在 spark web ui 页面的storage页签查看缓存情况
    // cache 的数据是在 executor 的内存里
    val rdd = sc.textFile("cll-spark/data/access.log")

    /*
     *
     * Persist this RDD with the default storage level (`MEMORY_ONLY`).
     * def cache(): this.type = persist()
     */
    // cache 是 persist 的一种特殊的方式 persist(StorageLevel.MEMORY_ONLY)
    rdd.cache() // def cache(): this.type = persist() = persist(StorageLevel.MEMORY_ONLY)

    // ★ lazy 懒加载 不会立即执行 生产用的比较多的也就是以下两种方式
    rdd.persist(StorageLevel.MEMORY_ONLY) // 仅仅缓存在内存中
    rdd.persist(StorageLevel.MEMORY_ONLY_SER) // 仅仅缓存在内存中  支持序列化

    rdd.persist(StorageLevel.MEMORY_AND_DISK) // 缓存在内存+磁盘中
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER) // 缓存在内存+磁盘中+序列化

    // 是否采用序列化 需要考虑到CPU 序列化会增加CPU的压力

    // 官网说明最好不要使用 _SER 从磁盘读取可能会比 .persist(StorageLevel...._SER) 更快
    // Don’t spill to disk unless the functions that computed your datasets are expensive, or they filter a large amount of the data.
    // Otherwise, recomputing a partition may be as fast as reading it from disk.

    // _2 一般也不回选择两副本缓存

    // 清空缓存
    rdd.unpersist() // ★ eager 立即执行的

    sc.stop()
  }

}
