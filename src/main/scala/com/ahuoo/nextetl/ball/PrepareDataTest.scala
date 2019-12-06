package com.ahuoo.nextetl.ball

import com.ahuoo.nextetl.BaseApp
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.Source

object PrepareDataTest extends BaseApp{

  import spark.implicits._

  val folder = "file:///C:/w/software/eclipse-jee/workspace/SparkETL/src/test/resources"

  def run(): Unit = {
    //prepare data for s3
    val df = readMysql("ball.increase_temp").cache()
    df.printSchema()
    df.createOrReplaceTempView("t_raw_data")
    writeParquet(df,s"$folder/other2/",5)


     //prepare data for testing
    val sql = getSql("/sql/PrepareData2.sql")
    val outputDf = spark.sql(sql).cache()
    println(outputDf.count())
    outputDf.show()
    writeCSV(outputDf,s"$folder/parquet",10000)
    //update filename
    import org.apache.hadoop.fs._
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val file = fs.globStatus(new Path(s"$folder/parquet/*/*.csv"))
    file.foreach(f=> {
      val path =  f.getPath.toString
      val pattern = ".+newId=(\\d+).+(.+\\.csv)".r
      val pattern(id,filename) = path
      fs.rename(f.getPath, new Path(s"$folder/parquet-final/$id.csv"))
    })


    //code in glue
/*    val df1 = readParquet("s3://ahuoo-bs/raw-data/").cache()
    df1.createOrReplaceTempView("t_raw_data")
    df1.show()
    df1.printSchema()
    val sql = getSql("/sql/PrepareData2.sql")
    val outputDf = spark.sql(sql).cache()
    outputDf.createOrReplaceTempView("o")
    println(outputDf.count())
    outputDf.show()
    writeCSV(outputDf,"s3://ahuoo-bs/output-men-women/",3)*/

  }



  def getSchemaDefinition(df: DataFrame): String={
    var schema = "val schema = new StructType()\n"
    df.schema.foreach(f=>{
      //      println(f.name)
      f.dataType match {
        case DataTypes.StringType => schema += ".add($\""+f.name+"\".string)\n"
        case DataTypes.IntegerType => schema += ".add($\""+f.name+"\".int)\n"
        case DataTypes.LongType => schema += ".add($\""+f.name+"\".long)\n"
        case DataTypes.DoubleType => schema += ".add($\""+f.name+"\".double)\n"
        case _ => log.error("The type was not defined for field: " + f)
      }
    })
    println(schema)
    schema
  }

  def readMysql(tableName: String): DataFrame ={
    log.info(tableName)
    val df = spark.read.format("jdbc").options(Map(
      "url" -> config.getString("mysql_test_url"),
      "dbtable" -> tableName
      ,"lowerBound" -> "8566346",
      "upperBound" -> "10310164",
      "numPartitions" -> "100",
      "partitionColumn" -> "id"
    )).load()
    df
  }

  def writeMysql(df: DataFrame, table: String): Unit ={
    val url = "jdbc:mysql://127.0.0.1:3306/ball?user=root&password=top960310A&serverTimezone=GMT&useUnicode=true&characterEncoding=utf8"
    df.repartition(100).write.format("jdbc").options(
      Map("url" -> url,
        "dbtable" -> table,
        "truncate" -> "true",
        "batchsize"->"10000"))
      .mode(SaveMode.Overwrite).save()
  }

  def readCSV(filename: String): DataFrame ={
    import spark.implicits._
    val schema = new StructType()
      .add($"id".int)
      .add($"updatedt".string)
      .add($"date".string)
      .add($"competitionName".string)
      .add($"team1".string)
      .add($"team2".string)
      .add($"currentScore1".int)
      .add($"currentScore2".int)
      .add($"offsetTime".int)
      .add($"first1".int)
      .add($"second1".int)
      .add($"third1_".long)
      .add($"forth1_".long)
      .add($"first2".int)
      .add($"second2".int)
      .add($"third2_".long)
      .add($"forth2_".long)
      .add($"betScore".double)
      .add($"lastScore".double)
      .add($"label".int)

    val df = spark.read.option("header", true)
      .option("delimiter", ",")
      .option("dateFormat", "yyyyMMdd")
      .schema(schema)
      .csv(filename)

    df
  }

  def writeCSV(df : DataFrame, filename: String, parNum: Int): Unit ={
    try{
/*      df.repartition(parNum)
        .write
        .format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("header", "true")
        .option("compression", "gzip")
        .save(s"$filename")*/

      df.write
        .partitionBy("newId")
        .format("com.databricks.spark.csv")
        .mode("overwrite")
       // .option("header", "true")
        .save(s"$filename")
    }catch{
      case e: Throwable => throw new Exception("Failed to generate csv file", e)
    }
  }

  def readParquet(filename: String): DataFrame ={
    spark.read.format("parquet").load(filename)
  }


  def writeParquet(df : DataFrame, filename: String, parNum: Int): Unit ={
    try{
      df.repartition(parNum).write.mode(SaveMode.Overwrite).option("compression", "gzip").parquet(filename)
    }catch{
      case e: Throwable => throw new Exception("Failed to generate parquet file", e)
    }
  }




  def getSql(path : String) = {
    val stream = getClass.getResourceAsStream(path)
    val sql = try Source.fromInputStream(stream).mkString finally stream.close()
    sql
  }

}

