package com.ahuoo.nextetl.log

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object LogTest {
  def main(args: Array[String]): Unit = {
    @transient lazy val log = Logger.getLogger(this.getClass)

    val defaultConfig = ConfigFactory.parseResources("default.conf")
    val config = ConfigFactory.parseResources("tables.conf").withFallback(defaultConfig).resolve().getConfig("NextETL")
    val debug = config.getBoolean("debug")
    val spark = if(debug) { SparkSession.builder.appName(config.getString("app_name")).master("local[3]").getOrCreate() }
    else { SparkSession.builder.appName(config.getString("app_name")).getOrCreate() }


    println("Spark Version: " + spark.version)
    println("Scala Version: " + util.Properties.versionNumberString)
    println("Java Version: " + System.getProperty("java.version"))
    log.info("Hello demo")

    val data = spark.sparkContext.parallelize(1 to 2000000)
    val mapper = Mapper(1)
    val other = mapper.doSomeMappingOnDataSetAndLogIt(data)
    other.collect()

    log.warn("I am done")

    val source = scala.io.Source.fromFile("ahuoo.log")
    val lines = try source.mkString finally source.close()
    println("Log:" + lines)
  }
}
