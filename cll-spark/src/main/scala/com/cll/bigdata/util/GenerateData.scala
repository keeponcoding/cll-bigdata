package com.cll.bigdata.util

import java.io.{File, PrintWriter}


/**
  * @ClassName GenerateData
  * @Description 生产数据
  * @Author cll
  * @Date 2019-12-10 14:40
  * @Version 1.0
  **/
object GenerateData {

  def main(args: Array[String]): Unit = {

    val writer = new PrintWriter(new File("cll-spark/file/a.txt"))

    writer.write("hello,scala,spark")

    writer.close()

  }

}
