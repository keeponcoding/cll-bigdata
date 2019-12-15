package com.cll.spark.core.testcase

import com.cll.spark.util.FileUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @ClassName MultiOutputApp
  * @Description 多目录输出
  * @Author cll
  * @Date 2019-12-15 09:05
  * @Version 1.0
  **/
object MultiOutputApp {

  val output_path = "cll-spark/out/multi/"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //
    val srcRDD = sc.textFile("cll-spark/data/access-info.log")

    // 判断输出目录是否存在
    FileUtil.deleteTarget(output_path, new Configuration())

    srcRDD.map(x => {
      val a = x.split("\t")
      (a(1),x) // tuple(platform,log)
    })
      .partitionBy(new HashPartitioner(5))
      //.saveAsTextFile(output_path)
      .saveAsHadoopFile(
          output_path,     // 输出路径
          classOf[String], // key 值类型
          classOf[String], // value 值类型
          classOf[LogMultipleTextOutputFormat], // 输出类型
          classOf[GzipCodec] // 压缩类型
      )

    sc.stop()
  }

  /*
   * 自定义多目录输出类
   * 实现 generateFileNameForKeyValue 方法
   */
  class LogMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any]{

    /*
     * 组装输出样式
     */
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
      s"$key/$name" // platform/filename
    }

    /*
     * 去除文件内的第一列platform 也就是key
     */
    override def generateActualKey(key: Any, value: Any): Any = {
      NullWritable.get()
    }
  }

}
