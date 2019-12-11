package com.cll.bigdata.datasource.ext.text.v2

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, In, IsNotNull, IsNull, LessThan}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/**
  * @ClassName MySQLSourceReader
  * @Description TODO
  * @Author cll
  * @Date 2019-12-09 16:02
  * @Version 1.0
  **/
class MySQLSourceReader(options:Map[String,String]) extends DataSourceReader
  with SupportsPushDownRequiredColumns  // 列裁剪
  with SupportsPushDownFilters // 过滤
{

  var requiredSchema: StructType = {
    val jdbcOptions = new JDBCOptions(options)
    JDBCRDD.resolveTable(jdbcOptions)
  }

  override def readSchema(): StructType = {
    requiredSchema
  }

  // 分片读取
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    //List[InputPartition[InternalRow]](MySQLInputPartition(requiredSchema, supportedFilters.toArray, options)).asJava
    null
  }

  // 列裁剪
  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer[Filter]()

  // 过滤
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (filters.isEmpty) {
      return filters
    }

    val unsupportedFilters = ArrayBuffer[Filter]()

    filters.foreach {
      case f: EqualTo => supportedFilters += f  // 等于
      case f: GreaterThan => supportedFilters += f // 大于
      case f: LessThan => supportedFilters += f // 小于
      case f: IsNull => supportedFilters += f // 为空
      case f: IsNotNull => supportedFilters += f // 不为空
      case f: In => supportedFilters += f // in 语法
      case f@_ => unsupportedFilters += f
    }

    unsupportedFilters.toArray
  }

  // 过滤
  override def pushedFilters(): Array[Filter] = supportedFilters.toArray
}
