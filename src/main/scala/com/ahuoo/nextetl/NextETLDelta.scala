package com.ahuoo.nextetl

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util.Calendar
import java.util.concurrent.Executors

import com.ahuoo.nextetl.NextETLBaseline.{config, filePath, log}
import com.ahuoo.nextetl.utils.ConfigUtil
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

case class Col(name: String)

object  NextETLDelta  extends BaseApp{
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
    lg("Hello NextETLDelta.")
    lg("Spark Version: " + spark.version)
    lg("Scala Version: " + util.Properties.versionNumberString)
    lg("Java Version: " + System.getProperty("java.version"))
    case class Table(sourceTable: String, targetTable: String, transfermationSql: String)

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
        case Success(v)=> lg(s"-------------Task Finished. Result: $v-------------")
        case Failure(e)=> lg(s"-------------Task Finished. Error : $e-------------")
      }
      future
    })
    val result = Future.sequence(futures)
    Await.ready(result, Duration.Inf)
    // lg(result.value)
    var errorCount = 0
    result.value.getOrElse(0) match {
      case Success(v) => {
        v.asInstanceOf[ArrayBuffer[(String, String, Double)]].foreach(t => lg("TableName: " + t._1 + ", Result: " + t._2 + ", Duration(min): " + t._3.formatted("%.2f")))
        errorCount = v.asInstanceOf[ArrayBuffer[(String, String, Double)]].count(t=>t._2.contains("ERROR"))
      }
      case Failure(s) => lg(s"Failed, message is: $s")
    }
    if(errorCount>0){
      throw new Exception(s"This Job contains $errorCount errors, please verify")
    }
    pool.shutdown()
    spark.stop()
  }


  def etl(sourceTable: String, targetTable: String,  tempTable: String, tranformationSQL: String): Future[(String, String, Double)] = Future{
    val startTime = Calendar.getInstance.getTime
    var result  =  "ERROR"
    try {
      val cols = getPrimaryKey(targetTable)
      if(cols.length==0){
        lg(s"The $targetTable doesn't have primary key.")
      }
      val pkColumns = cols.map(v=> v.name).mkString(",")
      val pkCondition = cols.map(v=> "a." +v.name + "=" + "b." + v.name).mkString(" AND ")
      val sourceDf = extractFromSource(sourceTable, tempTable, tranformationSQL)
      val targetDf = extractFromTarget(targetTable)
      val insertDf = sourceDf.except(targetDf).cache()
      val deleteDf = targetDf.except(sourceDf).cache()
      sourceDf.createOrReplaceTempView(s"source_$tempTable")
      targetDf.createOrReplaceTempView(s"target_$tempTable")
      // write to DB , UPDATE will be converted to delete and insert
      val insertTable = targetTable.replace("dbo.","ist.")
      val deleteTable = targetTable.replace("dbo.","del.")
      deleteDf.take(3)
      print(deleteDf.count())
      //save(deleteDf,deleteTable)
      //save(insertDf, insertTable)
      // update target DF for next time comparision
      val columns = targetDf.columns.mkString(",")
      val insertUnionDeleteDf = insertDf.union(deleteDf).cache()
      insertUnionDeleteDf.createOrReplaceTempView(s"insert_delete__$tempTable")
      val sqlExceptInsertAndDelete = s"select $columns from target_$tempTable where ($pkColumns) not in(select $pkColumns from insert_delete__$tempTable)"
      val newTargetDf = spark.sql(sqlExceptInsertAndDelete).union(insertDf).cache()
      result =  insertUnionDeleteDf.count().toString
      //make sure the write and db action to the last steps, otherwise , it has impact on df.count()
      if(insertUnionDeleteDf.takeAsList(1).size() > 0){
        //update DB
        val sqlDelete = s"delete from $targetTable a where exists(select 1 from  $deleteTable b where $pkCondition )"
        val sqlInsert = s"insert into $targetTable select * from $insertTable"
        dbExecute(List(sqlDelete, sqlInsert))
        //update Benchmark
        newTargetDf.write.mode(SaveMode.Overwrite).parquet(s"$filePath$targetTable/")
        lg(s"Refreshed target parquet file successfully. table: $targetTable")
      }else{
        lg(s"No data updated in source. table: $targetTable")
      }
      //clear cache
      insertDf.unpersist()
      deleteDf.unpersist()
      insertUnionDeleteDf.unpersist()
      newTargetDf.unpersist()
    }
    catch {
      case e : Throwable => {
        e.printStackTrace()
        result =  "ERROR in etl method: " + e.toString()
      }
    }
    val endTime = Calendar.getInstance.getTime
    (targetTable, result, (endTime.getTime-startTime.getTime)/1000.0/60.0)
  }

  def extractFromTarget(targetTable: String): DataFrame ={
    var df: DataFrame = null
    try{
      lg(s"Start to read data from parquet file from S3, path: $filePath$targetTable/")
      df = spark.read.format("parquet").load(s"$filePath$targetTable/")
    }catch{
      case e: Throwable => {
        lg("WARN : loading parquet file failed, load the full data from target database directly.")
        val url = config.getString("jdbc_target_url")
        df = spark.read.format("jdbc").options(Map("url" -> url,  "dbtable" -> targetTable)).load()
        df.write.mode(SaveMode.Overwrite).parquet(s"$filePath$targetTable/")
        lg(s"All data are written to parquet file, Path: $filePath$targetTable/")
      }
    }
    val parLen = df.rdd.partitions.length
    lg(s"End to read target Dataframe from parquet file, RDD partitions:$parLen, targetTable: $targetTable")
    df.coalesce(1)
  }

  def extractFromSource(sourceTable: String, tempTable: String, tranformationSQL: String): DataFrame ={
    val url = config.getString("jdbc_source_url")
    val sourceDf = spark.read.format("jdbc").options(Map("url" -> url,  "dbtable" -> sourceTable, "fetchsize"->"10000")).load()
    if(tranformationSQL!=""){
      sourceDf.createOrReplaceTempView(tempTable)
      lg(s"Start to transform for source table: $sourceTable" )
      val newDf = spark.sql(tranformationSQL)
      lg(s"End to transform for source table: $sourceTable" )
      newDf.coalesce(1)
    }else{
      sourceDf.coalesce(1)
    }
  }

  def getStringOrElse(config: Config,  key: String): String ={
    var result: String = ""
    try{
      result = config.getString(key)
    }catch{
      case e: Throwable => {
        lg("WARN : " + e.toString());
      }
    }
    return result
  }

  def save(df: DataFrame, targetTable: String): Unit ={
    val url = config.getString("jdbc_target_url")
    lg(s"Start to write data to db. TableName=$targetTable, PartitionSize=" + df.rdd.partitions.length)
    df.write.format("jdbc")
      .options(Map("url" -> url, "dbtable" -> targetTable,"truncate" -> "true","batchsize"->"10000"))
      // .mode(SaveMode.Append)
      .mode(SaveMode.Overwrite)
      .save()
    lg(s"Saved successfully.tableName=$targetTable")
    // val jdbcDF = spark.read.format("jdbc").options(Map("url" -> url,  "dbtable" -> targetTable)).load()
    // lg(s"Retrieve data from postgresql for $targetTable, rowcount=" + jdbcDF.count())
  }

  def lg(message: Any): Unit ={
    val dateTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    println(dateTime.format(formatter) + s" $message")
  }

  def dbExecute(sqlList: List[String]): Unit ={
    val url = config.getString("jdbc_target_url")
    val options = new JDBCOptions((Map("url" -> url,"dbtable" -> "dbo.a", "batchsize"->"10000")))
    val conn = JdbcUtils.createConnectionFactory(options)()
    conn.setAutoCommit(false)
    try{
      for((sql, idx) <- sqlList.zipWithIndex){
        lg(s"Start to execute:  $sql")
        val statement = conn.prepareStatement(sql)
        var num = statement.executeUpdate()
        lg(s"Affected rowcount: $num")
        statement.close()
      }
      conn.commit()
      lg("DB updated successfully.")
    }catch{
      case e: Throwable => {
        lg("ERROR : Start to rollback. " + e.toString());
        conn.rollback()
        throw new Exception("Error in dbExecute method.")
      }
    } finally {
      conn.close()
    }
  }

  def getPrimaryKey(targetTable: String): Seq[Col] = {
    val url = config.getString("jdbc_target_url")
    val options = new JDBCOptions((Map("url" -> url, "dbtable" -> "dbo.a", "batchsize" -> "10000","driver" -> "org.postgresql.Driver")))
    val sql = s"select kcu.table_schema,kcu.table_name, tco.constraint_name,kcu.ordinal_position as position,kcu.column_name as key_column " +
      s" from information_schema.table_constraints tco join information_schema.key_column_usage kcu on kcu.constraint_name = tco.constraint_name and kcu.constraint_schema = tco.constraint_schema  and kcu.constraint_name = tco.constraint_name"+
      s" where tco.constraint_type = 'PRIMARY KEY' and kcu.table_schema||'.'||kcu.table_name='$targetTable' "+
      s" order by kcu.table_schema, kcu.table_name,  position"

    val conn2 = JdbcUtils.createConnectionFactory(options)()
    val statement = conn2.createStatement
    val list = new scala.collection.mutable.ListBuffer[Col]
    try {
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        val keyCol = rs.getString("key_column")
        lg(s"$targetTable's PK column: $keyCol")
        list += new Col(keyCol)
      }
    } finally {
      statement.close()
      conn2.close()
    }
    list
  }
}
