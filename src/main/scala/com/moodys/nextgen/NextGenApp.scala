package com.moodys.nextgen

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession

object NextGenApp {
  val spark = SparkSession.builder.appName("GlueApp").master("local[3]").getOrCreate()

  def main(sysArgs: Array[String]): Unit ={
    spark.sparkContext.setLogLevel("ERROR")
    lg("Hello Glue.")
    lg("Spark Version: " + spark.version)
    lg("Scala Version: " + util.Properties.versionNumberString)
    lg("Java Version: " + System.getProperty("java.version"))
  }

  def lg(message: Any): Unit ={
    val dateTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    println(dateTime.format(formatter) + s" $message")
  }
}
