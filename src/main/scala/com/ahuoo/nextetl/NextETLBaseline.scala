package com.ahuoo.nextetl

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util.Calendar
import java.util.concurrent.Executors

import com.ahuoo.nextetl.utils.ConfigUtil
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Random, Success}

case class Table(sourceTable: String, targetTable: String, transfermationSql: String)

object NextETLBaseline extends BaseApp{

  val pool = Executors.newFixedThreadPool(5)
  implicit  val ec = ExecutionContext.fromExecutor(pool)
  var filePath: String = _
  val tables = List("dbo.test")

  override  def init(args: Array[String]): Unit = {
    super.init(args)
    filePath = config.getString("benchmark_file_path")
  }

  def run(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    log.info("Hello NextETLBaseline.")
    log.info("Spark Version: " + spark.version)
    log.info("Scala Version: " + util.Properties.versionNumberString)
    log.info("Java Version: " + System.getProperty("java.version"))
    val tableListConfig = config.getConfig().getList("table_list")
    val tableList = tableListConfig.map(config => {
      val element = config.asInstanceOf[ConfigObject].toConfig
      val sourceTable = element.getString("source_table")
      val targetTable = element.getString("target_table")
      var trans_sql = getStringOrElse(element, "trans_sql")
      new Table(sourceTable,targetTable,trans_sql)
    })
    var runTableList = tableList
    if(tables.length > 0){
      runTableList = tableList.filter(t => tables.contains(t.targetTable))
    }

    val futures =  runTableList.map(t => {
      val sourceTable = t.sourceTable
      val targetTable = t.targetTable
      var trans_sql = t.transfermationSql
      //not set tempTable value, use the sourceTable name as the tempTable
      val pattern = "\\s*\\(.*\\)\\s*(.+)".r
      val aliasTable =  sourceTable match { case pattern(tableName) => tableName case _ => sourceTable }
      val dateTime = LocalDateTime.now()
      val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      val suffix = Random.alphanumeric.take(5).mkString //dateTime.format(formatter)
      val tempTable = aliasTable.replaceAll("[\\[|\\]|\\.|#]","_")+"_"+suffix
      trans_sql = trans_sql.replace(aliasTable, tempTable)
      val future: Future[(String, String, Double)]= etl(sourceTable,targetTable, tempTable, trans_sql)
      future onComplete {
        case Success(v)=> log.info(s"-------------Task Finished. Result: $v-------------")
        case Failure(e)=> log.info(s"-------------Task Finished. Error : $e-------------")
      }
      future
    })
    val result = Future.sequence(futures)
    Await.ready(result, Duration.Inf)
    // log.info(result.value)
    var errorCount = 0
    result.value.getOrElse(0) match {
      case Success(v) => {
        v.asInstanceOf[ArrayBuffer[(String, String, Double)]].foreach(t => log.info("TableName: " + t._1 + ", Result: " + t._2 + ", Duration(min): " + t._3.formatted("%.2f")))
        errorCount = v.asInstanceOf[ArrayBuffer[(String, String, Double)]].count(t=>t._2.contains("ERROR"))
      }
      case Failure(s) => log.info(s"Failed, message is: $s")
    }
    if(errorCount>0){
      throw new Exception(s"This Job contains $errorCount errors, please verify")
    }
    pool.shutdown()
    spark.stop()
  }

  def getStringOrElse(config: Config,  key: String): String ={
    var result: String = ""
    try{
      result = config.getString(key)
    }catch{
      case e: Throwable => {
        log.info("WARN : " + e.toString());
      }
    }
    return result
  }

  def etl(sourceTable: String, targetTable: String,  tempTable: String, tranformationSQL: String): Future[(String, String, Double)] = Future{
    val startTime = Calendar.getInstance.getTime
    var result  =  "ERROR"
    try {
      val sourceDf = extract(sourceTable)
      if(tranformationSQL!=""){
        sourceDf.createOrReplaceTempView(tempTable)
        log.info(s"Start to transform for table: $sourceTable, tranformationSQL: $tranformationSQL" )
        val newDf = spark.sql(tranformationSQL)
        log.info(s"End to transform for table: $sourceTable" )
        save(newDf, targetTable)
        result = newDf.count().toString
      }else{
        save(sourceDf, targetTable)
        result = sourceDf.count().toString
      }
    }
    catch {
      case e : Throwable => {
        log.info("ERROR : " + e.toString());
        result = "ERROR: " + e.toString()
        // throw new Exception(e)
      }
    }
    val endTime = Calendar.getInstance.getTime
    (targetTable, result, (endTime.getTime-startTime.getTime)/1000.0/60.0)
  }

  def extract(sourceTable: String): DataFrame ={
    val url = config.getString("jdbc_source_url")
    val df = spark.read.format("jdbc").options(Map("url" -> url,  "dbtable" -> sourceTable, "fetchsize"->"10000")).load()
    df
  }

  def save(df: DataFrame, targetTable: String): Unit ={
    //write benchmark to s3
    df.write.mode(SaveMode.Overwrite).parquet(s"$filePath$targetTable/")
    //to db
    val url = config.getString("jdbc_target_url")
    log.info(s"Start to write data to db. TableName=$targetTable, PartitionSize=" + df.rdd.partitions.length)
    df.write.format("jdbc")
      .options(Map("url" -> url, "dbtable" -> targetTable, "truncate" -> "true","batchsize"->"10000"))
      // .mode(SaveMode.Append)
      .mode(SaveMode.Overwrite)
      .save()
    log.info(s"Saved successfully.tableName=$targetTable")
    // val jdbcDF = spark.read.format("jdbc").options(Map("url" -> url,  "dbtable" -> targetTable)).load()
    // log.info(s"Retrieve data from postgresql for $targetTable, rowcount=" + jdbcDF.count())
  }

 
}
