package com.ahuoo.nextetl.learn.scala

/**
  * Iterator               <=>     java.util.Iterator
  * Iterator               <=>     java.util.Enumeration
  * Iterable               <=>     java.lang.Iterable
  * Iterable               <=>     java.util.Collection
  * mutable.Buffer         <=>     java.util.List
  * mutable.Set            <=>     java.util.Set
  * mutable.Map            <=>     java.util.Map
  * mutable.ConcurrentMap  <=>     java.util.concurrent.ConcurrentMap
  */
object ListConvertTest {
  def main(args: Array[String]): Unit ={
    //create list
    import collection.JavaConverters._
    val jul = List(1, 2, 3).asJava

    //map ,transformation
    println(jul)

  }
}
