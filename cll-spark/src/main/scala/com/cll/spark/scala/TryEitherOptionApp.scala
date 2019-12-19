package com.cll.spark.scala

import scala.collection.mutable
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

/**
  * @ClassName TryEitherOptionApp
  * @Description Try Either Option
  * @Author cll
  * @Date 2019-12-19 08:19
  * @Version 1.0
  **/
object TryEitherOptionApp {

  def main(args: Array[String]): Unit = {
    /*
     * Try
     *
     * The `Try` type represents a computation that may either result in an exception,
     * or return a successfully computed value
     *
     * note1 : only non-fatal exceptions are caught by the combinators,Serious system errors, on the other hand, will be thrown
     * note2 : all Try combinators will catch exceptions and return failure unless otherwise specified in the documentation.
     */
    //println(Try(1/0).getOrElse("null"))
    //println(Try(0/1).getOrElse("null"))
    //println(divide)


    /*
     * Either
     * A common use of Either is as an alternative to [[scala.Option]] for dealing
     * with possible missing values.  In this usage, [[scala.None]] is replaced
     * with a [[scala.util.Left]] which can contain useful information.
     * [[scala.util.Right]] takes the place of [[scala.Some]].  Convention dictates
     * that Left is used for failure and Right is used for success.
     *
     */
    //either()



    /*
     * Option
     * Represents optional values. Instances of `Option` are either an instance of $some or the object $none.
     */
    option()
  }

  /*
   * Try
   * 官方案例
   */
  def divide: Try[Int] = {
    val dividend = Try(StdIn.readLine("Enter an Int that you'd like to divide:\n").toInt)
    val divisor = Try(StdIn.readLine("Enter an Int that you'd like to divide by:\n").toInt)
    val problem = dividend.flatMap(x => divisor.map(y => x/y))
    problem match {
      case Success(v) =>
        println("Result of " + dividend.get + "/"+ divisor.get +" is: " + v)
        Success(v)
      case Failure(e) =>
        println("You must've divided by zero or entered something that's not an Int. Try again!")
        println("Info from the exception: " + e.getMessage)
        divide
    }
  }

  /*
   * Either
   */
  def either1(): Unit ={
    val in = Console.readLine("Type Either a string or an Int: ")
    val result: Either[String,Int] = try {
        Right(in.toInt)
      } catch {
        case e: Exception =>
            Left(in)
      }

     println( result match {
       case Right(x) => "You passed me the Int: " + x + ", which I will increment. " + x + " + 1 = " + (x + 1)
       case Left(x) => "You passed me the String: " + x
     })
  }

  /*
   * Either
   */
  def either(): Unit ={
    val l: Either[String, Int] = Left("flower")
    val r: Either[String, Int] = Right(12)
    println(l.left.map(_.size): Either[Int, Int]) // Left(6)
    println(r.left.map(_.size): Either[Int, Int]) // Right(12)
    println(l.right.map(_.toDouble): Either[String, Double]) // Left("flower")
    println(r.right.map(_.toDouble): Either[String, Double]) // Right(12.0)
  }

  /*
   * option
   */
  def option(): Unit = {
    val params = mutable.HashMap{
      "name" -> "zhangsan"
      "age" -> 18
      "code" -> 1001
    }
    val addr = params.get("addr")
    addr match {
      case Some(a) => println(a)
      case None => println("没有该参数")
    }
  }

}
