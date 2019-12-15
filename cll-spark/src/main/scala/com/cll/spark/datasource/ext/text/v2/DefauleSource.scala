package com.cll.spark.datasource.ext.text.v2

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

/**
  * @ClassName DefauleSource
  * @Description 基于 data source v2 版本的 defaultsource
  *             ReadSupport 读逻辑
  *             WriteSupport 写逻辑
  * @Author cll
  * @Date 2019-12-09 15:59
  * @Version 1.0
  **/
class DefauleSource extends DataSourceV2 with ReadSupport{


  override def createReader(options: DataSourceOptions): DataSourceReader = {
    null
  }


}
