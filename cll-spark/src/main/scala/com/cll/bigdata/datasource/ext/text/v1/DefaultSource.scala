package com.cll.bigdata.datasource.ext.text.v1

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * @ClassName DefaultSource
  * @Description 自定义处理外部数据源
  *             该类名称 不能乱写
  * @Author cll
  * @Date 2019-12-05 22:51
  * @Version 1.0
  **/
class DefaultSource extends RelationProvider with SchemaRelationProvider{

  // SchemaRelationProvider 中的 createRelation
  def createRelation(sqlContext: SQLContext,parameters: Map[String, String],schema: StructType): BaseRelation = {
    val path = parameters.get("path")

    path match {
      case Some(p) => new TextDataSourceRelation(sqlContext, p, schema)
      case _ => throw new IllegalArgumentException("Parameters path is required，but this is null")
    }
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

}
