package com.ahuoo.nextetl.log

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
  * This code will be executed in executors.  it illustrates how to use log4j in executors.
  * @param n
  */
class Mapper(n: Int) extends Serializable{
  @transient lazy val log = Logger.getLogger(this.getClass)
  def doSomeMappingOnDataSetAndLogIt(rdd: RDD[Int]): RDD[String] =
    rdd.map{ i =>
      log.warn("mapping: " + i)
      (i + n).toString
    }
}
object Mapper {
  def apply(n: Int): Mapper = new Mapper(n)
}