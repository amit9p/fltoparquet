package com.capitalone.flparquet.converter.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import com.capitalone.flparquet.converter.constants._
import scala.io.Source
import scala.collection.mutable.ListBuffer
import java.io.File
import java.io.PrintWriter
import com.capitalone.flparquet.converter._

class functions {

  var flData = scala.collection.mutable.Map[String, List[String]]()

  def getSparkSession(): SparkSession = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
    return sparkSession
  }

  def createExceldf(file: String, spark: SparkSession, sheet: String): DataFrame = {
    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", sheet) // Required
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .load(file)

    return df
  }

  def getSparkSqlResult(sql: String, spark: SparkSession, FieldName: String): Array[Any] = {

    val result = spark.sql(sql).select(FieldName).rdd.map(r => r(0)).collect()
    return result
  }

  def getSchemaMetadat(sqlDF: Array[Any]): scala.collection.mutable.Map[String, String] = {
    var schemaMetaMap = scala.collection.mutable.Map[String, String]()
    for (x <- sqlDF) {
      val data: String = x.toString()
      println(x)
      val stsql = getSparkSession().sql(sql.getStartLength + "'" + data + "'").select("StartLength").rdd.map(r => r(0)).collect()
      val endsql = getSparkSession().sql(sql.getEndLength + "'" + data + "'").select("EndLength").rdd.map(r => r(0)).collect()
      val combinedValue = stsql(0).toString().concat("-").concat(endsql(0).toString())
      schemaMetaMap += (data -> combinedValue)

    }
    return schemaMetaMap
  }

  def fldataparser(flpath: String, metaSchema: scala.collection.mutable.Map[String, String]): Unit = {
    var listOfData = new Array[ListBuffer[String]](getLineCount(flpath))
    var headers = new ListBuffer[String]()
    var size = 0
    var flag = false
    for (line <- Source.fromFile(flpath).getLines) {
      listOfData(size) = new ListBuffer[String]()
      for ((k, v) <- metaSchema) {

        val key = k
        val value = v

        if (flag == false) {
          headers += key

        }
        val splitValues = value.split("-").map(_.trim)
        val fetchLineData = line.substring(splitValues(0).toInt, splitValues(1).toInt).trim()
        listOfData(size) += fetchLineData.toString()
      }
      size = size + 1
      flag = true
    }

    createCsvFile(Client.csvFilePath, headers, listOfData)

  }

  def createCsvFile(csvFile: String, headers: ListBuffer[String], dataList: Array[ListBuffer[String]]): Unit = {
    val writer = new PrintWriter(new File(csvFile))

    for (header <- headers) {
      writer.write(header)
      writer.write("|")
    }
    writer.write("\n")

    for (data <- dataList) {
      for (lineData <- data) {
        writer.write(lineData)
        writer.write("|")
      }
      writer.write("\n")
    }

    writer.flush
    writer.close
    println("csv file created")
    csvToParquet(csvFile, headers)
  }

  def getLineCount(file: String): Int = {
    val lineCount = getSparkSession.sparkContext.textFile(file).count()
    return lineCount.toInt
  }

  def csvToParquet(csvFile: String, headers: ListBuffer[String]): Unit = {

    var columns = ""
    for (header <- headers) {
      columns = columns.concat(header).concat(",")
    }
    columns = columns.dropRight(1)
    val parquetDF = getSparkSession.read.format("csv").option("header", "true").option("delimiter", "|").load(csvFile)
    parquetDF.createTempView("csvtbl")
    getSparkSession.sql("SELECT " + columns + " FROM csvtbl").write.format("parquet").option("header", "true").save(Client.parquetFilePath)
    println("parquet file created")

  }

}