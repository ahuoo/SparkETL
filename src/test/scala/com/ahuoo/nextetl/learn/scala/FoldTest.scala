package com.ahuoo.nextetl.learn.scala

/**
  * http://allaboutscala.com/tutorials/chapter-2-learning-basics-scala-programming/learn-to-create-use-enumerations/
  */
object FoldTest  {

  def main(args: Array[String]): Unit ={
    var a = 0
    val numbers = List(1, 2, 3, 4)
    numbers.fold(0) { (z, i) =>
      a = a + z + i
      z + i
    }
    println(a)
  }
}