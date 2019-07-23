package com.ahuoo.nextetl

import org.apache.spark.sql.SparkSession

object VersionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("VersionTest").master("local[3]").getOrCreate()
//    val spark = SparkSession.builder.appName("VersionTest").getOrCreate()
    println("Spark Version: " + spark.version)
    println("Scala Version: " + util.Properties.versionNumberString)
    println("Java Version: " + System.getProperty("java.version"))
  }
}
