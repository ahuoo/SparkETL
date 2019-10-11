package com.ahuoo.nextetl

import com.ahuoo.nextetl.utils.ConfigUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait BaseApp {
  @transient lazy val log = Logger.getLogger(this.getClass)
  var config: ConfigUtil = _
  val spark =  SparkSession.builder.appName("NextETL").master("local[3]").getOrCreate()
  import spark.implicits._

  /**
    * override in your application object, if you need other variable to initialize, but please call super.init(args) first
    * @param args
    */
  def init(args: Array[String]) {
/*    config = new ConfigUtil(args)
    log.info("Args: " + args.mkString(" "))*/

    //only for quick debug
    val debugArgs = Array("dev","###project.env=dev###project.retryNum=0")
    config = new ConfigUtil(debugArgs)
    log.info("Args: " + debugArgs.mkString(" "))
  }

  def main(args: Array[String]): Unit = {
    init(args)
    run()
  }

  /**
    * it is a abstract method, must be implemented in subclass
    */
  def run()
}
