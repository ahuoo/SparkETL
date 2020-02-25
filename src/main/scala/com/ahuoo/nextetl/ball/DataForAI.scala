package com.ahuoo.nextetl.ball
import com.ahuoo.nextetl.BaseApp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime}
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object DataForAI extends BaseApp{


  import spark.implicits._
  //s3://ahuoo-bs
  val prefix = "file:///C:\\w\\software\\eclipse-jee\\workspace\\SparkETL\\src\\test\\resources"

  def run(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    lg("Hello Glue.")
    lg("Spark Version: " + spark.version)
    lg("Scala Version: " + util.Properties.versionNumberString)
    lg("Java Version: " + System.getProperty("java.version"))

    //prepare data for ai
    val schema = "`id` BIGINT,`team1` STRING,`team2` STRING,`offsetTimeStr` STRING,`offsetTime` INT,`date` STRING,`updateDt` STRING,`currentScore1` INT,`currentScore2` INT,`first1` INT,`second1` INT,`thrid1` INT,`forth1` INT,`first2` INT,`second2` INT,`thrid2` INT,`forth2` INT,`betScore` DOUBLE,`lastScore` DOUBLE,`increaseNum` DOUBLE,`competitionName` STRING,`maxBetScore` DOUBLE,`logFile` STRING,`lastScore1` INT,`lastScore2` INT,`orderScore` DOUBLE,`suspended` INT,`minBetScoreAfterOrder` DOUBLE"
    val df1 = readParquet(s"$prefix/raw-data").cache()
    df1.createOrReplaceTempView("t_raw_data")
    df1.show()
    df1.printSchema()
    val sql = "select  \n      gameId,updateDt\n     ,date,competitionName,team1,team2,currentScore1,currentScore2,offsetTime\n     ,first1,second1,third1,forth1\n     ,first2,second2,third2,forth2\n     ,betScore,lastScore,gender,overTime1,overTime2,lastThird1,lastForth1,lastThird2,lastForth2,case\n        when competitionName in  ('NCAAB','NCAAB Extra Games','国家邀请赛') then 2\n        when competitionName in  ('NBA季前赛','中国男子职业联赛','NBA','中国全国男子篮球联赛','菲律宾PBA总督杯','NBA G联赛') then 3\n        else 1\n     end as gameType\nFROM\n(\n\n  SELECT min(updateDt) over(partition by date,team1,team2,lastScore order by id asc) newId     \n     ,min(offsetTime) over(partition by date,team1,team2,lastScore order by id asc) minOffsetTime\n     ,min(updateDt) over(partition by date,team1,team2,lastScore order by id asc) gameId\t \n\t ,updateDt, date,competitionName,team1,team2,currentScore1,currentScore2,offsetTime\n    ,first1,second1     \n    ,case when offsetTime<=600 then (currentScore1-first1-second1) else thrid1 end as third1    \n    ,case when offsetTime>600 then (currentScore1-first1-second1-thrid1) else 0 end as forth1    \n    ,first2,second2\n    ,case when offsetTime<=600 then (currentScore2-first2-second2) else thrid2 end as third2   \n    ,case when offsetTime>600 then (currentScore2-first2-second2-thrid2) else 0 end as forth2  \n    ,betScore,lastScore,if(team1 LIKE '%女子%' OR team1 LIKE '%Women%',1,0) as gender\n    ,lastScore1-first1-second1-thrid1-forth1 as overTime1, lastScore2-first2-second2-thrid2-forth2 as overTime2\n    ,thrid1 as lastThird1,forth1 as lastForth1,thrid2 as lastThird2,forth2 as lastForth2\n    FROM t_raw_data\n    WHERE betScore>0  \n )t\n where t.minOffsetTime>=0\n  \n "
    val outputDf = spark.sql(sql).cache()
    println(outputDf.count())
    outputDf.show()
    writeCSV(outputDf,s"$prefix/output-men-women",1)
  }

  def readCSV(filename: String): DataFrame ={
    val schema = new StructType()
      .add($"id".int)
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
      .add($"gender".int)
      .add($"updatedt".string)

    val df = spark.read.option("header", true)
      .option("delimiter", ",")
      .option("dateFormat", "yyyyMMdd")
      .schema(schema)
      .csv(filename)

    df
  }

  def writeCSV(df : DataFrame, filename: String, parNum: Int): Unit ={
    try{
      df.repartition(parNum,$"date")
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

  def readParquet(filename: String,schema: String): DataFrame ={
    spark.read.format("parquet").schema(schema).load(filename)
  }



  def writeParquet(df : DataFrame, filename: String, parNum: Int): Unit ={
    try{
      df.repartition(parNum).write.mode(SaveMode.Overwrite).option("compression", "gzip").parquet(filename)
    }catch{
      case e: Throwable => throw new Exception("Failed to generate parquet file", e)
    }
  }



  def lg(message: Any): Unit ={
    val dateTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    println(dateTime.format(formatter) + s" $message")
  }
}
