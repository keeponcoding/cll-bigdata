package com.cll.flink.bean

/**
 * @ClassName Domain
 * @Description TODO
 * @Author cll
 * @Date 2020/2/10 2:22 下午
 * @Version 1.0
 **/
object Domain {

  case class WC(word:String, count:Int)

  case class Access(time:Long, site:String, traffic:Int)

}
