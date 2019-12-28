package com.cll.spark.project.offline

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @ClassName AppLogUtil
  * @Description 处理rdd
  * @Author cll
  * @Date 2019-12-28 21:33
  * @Version 1.0
  **/
object AppLogUtil {

  /**
    * 解析日志信息
    * @param log
    * @return
    */
  def parseLog(log: String): Row = {
    val p = log.split("\\|")
    try {
      Row(
        p(0), p(1), p(2), p(3), p(4)
        , p(5), p(6), p(7), p(8), p(9)
        , p(10), p(11), p(12), p(13), p(14)
        , p(15), p(16), p(17), p(18), p(19)
        , p(16), p(1).substring(0, 10), p(1).substring(11, 13)
      )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Row(0)
    }
  }

  /**
    * 获取schema
    * @return
    */
  def getSchema(): StructType = {
    StructType(
      Array(
        StructField("project_id", StringType),
        StructField("create_at", StringType),
        StructField("server_id", StringType),
        StructField("event_id", StringType),
        StructField("user_id", StringType),
        StructField("udid", StringType),
        StructField("platform", StringType),
        StructField("bundle_id", StringType),
        StructField("session_id", StringType),
        StructField("user_name", StringType),
        StructField("app_version", StringType),
        StructField("os_version", StringType),
        StructField("channel", StringType),
        StructField("locale_country", StringType),
        StructField("language", StringType),
        StructField("is", StringType),
        StructField("ip", StringType),
        StructField("idfa", StringType),
        StructField("idfv", StringType),
        StructField("google_id", StringType),
        StructField("country", StringType),
        StructField("day", StringType),
        StructField("hour", StringType)
      )
    )
  }

}
