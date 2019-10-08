package com.ahuoo.nextetl.learn.scala

object ListBufferTest {
  def main(args: Array[String]): Unit ={

    //create list that can be modified.
    import scala.collection.mutable.ListBuffer
    var fruits = new ListBuffer[String]()
    fruits += "Apple"
    fruits += "Banana"
    fruits += "Orange"

    //convert to scala List
    val fruitsList = fruits.toList
    println(fruitsList)

  }
}
