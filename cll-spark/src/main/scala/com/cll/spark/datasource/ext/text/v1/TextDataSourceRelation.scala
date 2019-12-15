package com.cll.spark.datasource.ext.text.v1

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * @ClassName TextDataSourceRelation
  * @Description 文本数据源
  * @Author cll
  * @Date 2019-12-05 22:52
  * @Version 1.0
  **/
class TextDataSourceRelation(override val sqlContext: SQLContext,path: String,userSchema: StructType)
  extends BaseRelation
    with TableScan // 全表扫描
    //with PrunedScan // TODO PrunedScan 裁剪
    //with PrunedFilteredScan // TODO PrunedFilteredScan 过滤
    with Serializable
    with Logging{

  override def schema: StructType = {
    if(userSchema != null){
      userSchema
    }else{
      StructType(
        StructField("id",LongType,false) ::
        StructField("name",StringType,false) ::
        StructField("gender",StringType,false) ::
        StructField("salary",LongType,false) ::
        StructField("comm",LongType,false) :: Nil
      )
    }
  }

  // TableScan : select * from xxx
  override def buildScan(): RDD[Row] = {
    logWarning("this is custome buildScan...")

    // ._1 是文件名   ._2是文件内容
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
    val schemaField = schema.fields

    // 处理文件内容
    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(_.split(",").map(_.trim)).toSeq // 该处toSeq 是为了给 ① 出使用

      val result = data.map(x => x.zipWithIndex.map{

        case (value, index) => {

          val columnName = schemaField(index).name

          castTo(if(columnName.equalsIgnoreCase("gender")){
            if(value == "0"){
              "女"
            }else if(value == "1"){
              "男"
            }else{
              "未知"
            }
          }else{
            value
          },schemaField(index).dataType)
        }
      })

      result.map(x => Row.fromSeq(x)) // ①
    })

    rows.flatMap(x => x)
  }

  // 类型转换
  def castTo(value:String, dataType: DataType) = {
    dataType match {
      case _:LongType => value.toLong
      case _:StringType => value
    }
  }

  /*// TODO PrunedScan : select a,b,c from xxx
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    /*
     * 参考 JDBCRDD.pruneSchema
     * 指定的列处理
     */
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    val structType = new StructType(requiredColumns.map(name => fieldMap(name)))



    null
  }

  // TODO PrunedFilteredScan : select a,b,c from xxx where a > 2
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    null
  }*/
}
