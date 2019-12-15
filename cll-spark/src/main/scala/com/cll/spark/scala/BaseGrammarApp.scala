package com.cll.spark.scala

/**
  * @ClassName BaseGrammarApp
  * @Description scala 基础语法
  * @Author cll
  * @Date 2019-12-09 18:25
  * @Version 1.0
  **/
object BaseGrammarApp {

  def main(args: Array[String]): Unit = {

    // 最大值  最小值
    /*val seq = Seq(1,2,3,4,5,6,7,3)
    println(seq.max)
    println(seq.min)
    println(seq.filter(_ > 4))*/

    //
    /*val emps = Seq(
      Emp("zhangsan",1000.20),
      Emp("lisi",10000),
      Emp("wanger",1000.0),
      Emp("mazi",1000.2)
    )
    println(emps.maxBy(emp => emp.salary))
    println(emps.minBy(emp => emp.salary))
    println(emps.filter(emp => emp.salary >= 10000))*/

    //
    /*val seq1 = Seq("hello","world","java")
    val seq2 = Seq("hello","scala")
    val seq3 = Seq("hello","spark")
    val seq4 = Seq("hello","hadoop")
    val seq5 = Seq("hello","flink")
    val seq6 = Seq("hello","kafka")

    val seq_all = Seq(seq1,seq2,seq3,seq4,seq5,seq6)
    println(seq_all.flatten)*/

    /*
     * 欧拉图函数 Euler Diagram
     * 差集 交集 并集
     */
    /*val num1 = Seq(1,2,3,4,5,6)
    val num2 = Seq(4,5,6,7,8,9)
    println(num1.union(num2)) // num1 num2 所有的元素 重复
    println(num1.union(num2).distinct) // num1 num2 所有的元素 不重复
    println(num1.diff(num2)) // num1 独有的部分
    println(num1.intersect(num2)) // num1 num2 共有的部分*/

    // flatMap = flatten + map

    // 集合拆分
    val seq = Seq(1,2,3,4,5,6,7,8,9)
    println(seq.partition(a => a % 2 == 0)) // (List(2, 4, 6, 8),List(1, 3, 5, 7, 9))


  }

  case class Emp(name:String, salary:Double)

}
