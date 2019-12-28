package com.cll.spark.project.offline

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * @ClassName LogTrain
  * @Description TODO
  * @Author cll
  * @Date 2019-12-28 16:32
  * @Version 1.0
  *         刷新hive分区 ALTER TABLE table_name ADD IF NOT EXISTS PARTITION (p1=..., p2=...);
  **/
object LogTrain {

  val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val sqlContext = spark.sqlContext

    // 输入文件
    val inputPath = sqlContext.getConf("spark.app.inputPath")
    // 输出文件
    val outputPath = sqlContext.getConf("spark.app.outputPath")
    // 输出文件个数
    val size = sqlContext.getConf("spark.app.outputFileSize").toInt
    // 获取FileSystem对象
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // STEP 1 读取原数据
    val srcRDD = spark.sparkContext.textFile(inputPath)
    // STEP 2 处理小文件
    val convertRDD = srcRDD.coalesce(getCoalesceNum(fs, inputPath, size))

    // STEP 3 rdd to df
    val df = spark.createDataFrame(convertRDD.map(x=>AppLogUtil.parseLog(x)),AppLogUtil.getSchema())

    // STEP 4 落地
    df.write.mode(SaveMode.Overwrite).partitionBy("").save(outputPath)

    // STEP 5 创建hive表 并刷新分区

    spark.stop()
  }

  /**
    * 获取coalesce数
    *
    * @param fs
    * @param inputPath
    * @param size
    * @return
    */
  def getCoalesceNum(fs: FileSystem, inputPath: String, size: Int): Int = {
    // 初始化文件大小
    var num = 0L

    // 返回指定路径下的所有文件
    val files = fs.globStatus(new Path(inputPath))

    // 将所有文件的大小相加
    files.foreach(x => {
      num += x.getLen
    })

    (num / 1024 / 1024 / size).toInt
  }

}
