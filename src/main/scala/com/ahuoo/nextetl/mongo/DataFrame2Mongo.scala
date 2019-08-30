package com.ahuoo.nextetl.mongo


import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, _}
import org.mongodb.scala.bson.{BsonDateTime, BsonDocument, BsonDouble, BsonInt32, BsonString}
import org.mongodb.scala.{MongoClient, MongoCollection}
import org.mongodb.scala.bson.collection.mutable.Document
import org.mongodb.scala.model.Filters.in
import scala.collection.mutable.WrappedArray
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object DataFrame2Mongo {
  @transient lazy val log = Logger.getLogger(this.getClass)
  val spark = SparkSession.builder.appName("").master("local[3]").getOrCreate()

  def parseRow2BsonDocument(row: Row): BsonDocument={
    var nestedDoc = BsonDocument()
    var index = 0
    for(field <- row.schema.fieldNames) {
      if (row.get(index) != null) {
        row.get(index) match {
          case v: String => nestedDoc.put(field, BsonString(v))
          case v: Double => nestedDoc.put(field, BsonDouble(v))
          case v: Int => nestedDoc.put(field, BsonInt32(v))
          case v: java.sql.Timestamp => nestedDoc.put(field, BsonDateTime(v))
          case v: java.sql.Date => nestedDoc.put(field, BsonDateTime(v))
          case _ => log.error("The type was not defined for field: " + field)
        }
      }
      index += 1
    }
    nestedDoc
  }

  def parseRow2Document(row: Row, doc: Document): Document={
    var index = 0
    for(field <- row.schema.fieldNames) {
      if (row.get(index) != null) {
        row.get(index) match {
          case v: String => doc.put(field, v)
          case v: Double => doc.put(field, v)
          case v: Int =>  doc.put(field, v)
          case v: java.sql.Timestamp =>  doc.put(field, v)
          case v: java.sql.Date =>  doc.put(field, v)
          case v: WrappedArray[String] =>  doc.put(field, row.getAs[List[String]](field))
          case v: Row => doc.put(field, parseRow2BsonDocument(v))
          case _ => log.error("The type was not defined for field: " + field)
        }
      }
      index += 1
    }
    doc
  }


  def main(args: Array[String]): Unit = {
    var rowList: List[Document] = Nil
    val stmtIdSet = Set("3324234")
    var doc = Document(
      "stmt_id" -> "12"
      ,"account_id" -> "1"
    )

    val data = Seq(
      Row("bob", Row("blue", 45)),
      Row("mary", Row("red", 64))
    )
    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField(
          "person_details",
          StructType(
            List(
              StructField("favorite_color", StringType, true),
              StructField("age", IntegerType, true)
            )
          ),
          true
        )
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.printSchema()
    df.take(1).foreach(r=> doc = parseRow2Document(r, doc))
    var doc2 = Document(
      "stmt_id" -> "3"
    )
    doc2.put("account_id","1")
    rowList = doc :: rowList
    rowList = doc2 :: rowList
    var mongoClient: MongoClient = null
    var collection: MongoCollection[Document] = null
    try{
      mongoClient = MongoClient("mongodb://eagle_exec:eagle123@10.14.77.31:27017,10.14.77.10:27017,10.14.77.56:27017/ODS?authSource=ODS&connectTimeoutMS=300000")
      collection = mongoClient.getDatabase("ODS").getCollection("bvd_statement_details_test222")
      val result = collection.deleteMany(in("stmt_id", stmtIdSet.toList:_*)).head()
      Await.result(result, Duration.Inf)
      val insertObservable = collection.insertMany(rowList).head()
      Await.result(insertObservable, Duration.Inf)
    }catch{
      case ex: Throwable =>  log.info("Error occurred when it tired to delete the documents", ex)
        throw ex
    }finally{
      if (mongoClient!=null){
        mongoClient.close()
      }
    }
  }
}
