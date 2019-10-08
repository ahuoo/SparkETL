package com.ahuoo.nextetl.learn.scala

/**
  * // asScala and asJava
  * scala.collection.mutable.Map    <=> java.util.Map
  * scala.collection.concurrent.Map <=> java.util.concurrent.ConcurrentMap
  *
  * // asScala, asJavaDictionary
  * scala.collection.mutable.Map <=> java.util.Dictionary
  *
  * // asJava
  * scala.collection.Map => java.util.Map
  */
object MapConvertTest {
    def main(args: Array[String]): Unit ={
      // import what you need
      import java.util._
      import scala.collection.JavaConverters._

      // create and populate a java map
      val javaMap = new HashMap[String, String]()
      javaMap.put("first_name", "Alvin")
      javaMap.put("last_name",  "Alexander")

      //java to scala, asScala
      val scalaMap = javaMap.asScala
      println(scalaMap)

      //scala to java, asJava
      val newJavaMap = scalaMap.asJava
      println(newJavaMap)
    }
}
