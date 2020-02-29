package com.cll.flink.bean

/**
 * @ClassName Domain
 * @Description TODO
 * @Author cll
 * @Date 2020/2/10 2:22 下午
 * @Version 1.0
 **/
object Domain {

  case class WC(word:String, cnt:Int)

  case class Access(time:Long, site:String, traffic:Int)

  case class Student(id:Int, name:String, age:Int)

  case class User(id:Int, name:String, order_no:Int)

}
