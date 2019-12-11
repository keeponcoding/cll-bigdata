package com.cll.bigdata.ss.offset

/**
  * @ClassName OffsetManager
  * @Description 偏移量统一管理接口
  * @Author cll
  * @Date 2019-12-10 10:56
  * @Version 1.0
  **/
trait OffsetManager {

  // 获取偏移量
  def obtain

  // 存储偏移量
  def store

}
