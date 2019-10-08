package com.ahuoo.nextetl.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

class  ConfigUtil(args: Array[String]) extends Serializable {

  @transient lazy val log = Logger.getLogger(this.getClass)
  if(args == null || args.length != 2){
    throw new Exception("You should input two parameters,like below: \n dev ###project.env=dev###project.retryNum=0")
  }
  val env: String  = args(0)
  val overrideArgs: String  = args(1)
  val defaultConfig = ConfigFactory.parseResources("default.conf")
  val config = ConfigFactory.parseResources(s"default."+env.toLowerCase()+".conf").withFallback(defaultConfig).resolve().getConfig("NextETL")
  var cache = scala.collection.mutable.Map[String,String]()

  /**
    * The priority of parameters from frontend is the highest
    * @param key
    * @return
    */
  def getConfigValue(key: String): Any ={
    val cacheValue = cache.get(key)
    if(!cacheValue.isEmpty){
      log.debug(s"Load config from cache, $key="+cacheValue.getOrElse("None"))
      return cacheValue.getOrElse("None")
    }
    //frontend
    val overrideParams = overrideArgs.split("###")
    val keyValuePairs = overrideParams.map(v=>v.toString.split("=",2))
    for(keyValue <- keyValuePairs){
      if(keyValue.size == 2){
        if(keyValue(0).replace("project.","").equalsIgnoreCase(key) && keyValue(1)!="NA" && keyValue(1)!=""){
          val result = keyValue(1).toString.replace("\\","")
          log.info(s"Load config from frontend, $key="+result)
          cache += (key->result)
          return result
        }
      }
    }
    //jar
    val result = config.getString(key)
    log.info(s"Load config from jar, $key="+result)
    cache += (key->result)
    if(result.isEmpty){
      log.error(s"No config found for $key")
      throw new Exception(s"No config value found for $key")
    }
    result
  }

  def getBoolean(key: String): Boolean ={
    val value = this.getConfigValue(key).toString
    value.toBoolean
  }

  def getString(key: String): String ={
    this.getConfigValue(key).toString
  }

  def getInt(key: String): Int ={
    val value = this.getConfigValue(key).toString
    value.toInt
  }

  def getConfig(): Config={
    config
  }

}
