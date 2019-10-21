package com.ahuoo.nextetl.ball

import com.ahuoo.nextetl.BaseApp
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source

object PrepareDataTest extends BaseApp{


  def run(): Unit = {
    val df = readParquet("file:///C:\\Users\\caih\\Downloads\\men-parquet")
    println(df.count())
    df.createOrReplaceTempView("t")
    val sql = getSql("/sql/PrepareData.sql")
    val outputDf = spark.sql(sql)
    outputDf.createOrReplaceTempView("o")
    println(outputDf.count())
    outputDf.cache()
/*    outputDf.schema.fieldNames.map(f => {
      println(s"===========$f=============")
      outputDf.describe(f).show()
//      outputDf.stat.approxQuantile(f, Array(0.25,0.5,0.75),0.0).foreach(println)
    })*/
    writeCSV(outputDf,"file:///C:\\Users\\caih\\Downloads\\men-parquet-output",10)



//    writeCSV(outputDf,"file:///C:\\Users\\caih\\Downloads\\men-parquet-output",3)

   // db2Csv("file:///c:/mdc-data/men-parquet")


/*
    writeMysql(df, "bs")
    df.show()
    df.printSchema()
    getSchemaDefinition(df)

    writeParquet(df,"file:///c:/mdc-data/ball0901-parquet")
    val dfParquet = readParquet("file:///c:/mdc-data/ball0901-parquet")
    println("Parquent row count: " + dfParquet.count())

    writeCSV(df,"file:///c:/mdc-data/ball0901-csv")
    val dfCsv = readCSV("file:///c:/mdc-data/ball0901-csv/part-*.csv")
    println("CSV row count: " + dfCsv.count())*/
  }


  def db2Csv(filename: String): Unit ={
    val sql = getSql("/sql/PrepareData.sql")
    val df = readMysql(sql)
    writeCSV(df,filename,5)
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

  def readMysql(sql: String): DataFrame ={
    log.info(sql)
    val df = spark.read.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://10.5.135.15:3306/ball?user=root&password=sino2009&useUnicode=true&characterEncoding=utf-8",
      "dbtable" -> s"($sql) t"
      ,"lowerBound" -> "4633",
      "upperBound" -> "198540654",
      "numPartitions" -> "100",
      "partitionColumn" -> "id"
    )).load()
    df
  }

  def writeMysql(df: DataFrame, table: String): Unit ={
    val url = "jdbc:mysql://10.5.135.15:3306/test?user=root&password=sino2009&serverTimezone=GMT&useUnicode=true&characterEncoding=utf8"
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
      df.repartition(parNum)
        .write
        .format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("header", "true")
        .option("compression", "gzip")
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

