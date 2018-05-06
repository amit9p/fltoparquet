package com.capitalone.flparquet.converter

import com.capitalone.flparquet.converter.common._
import com.capitalone.flparquet.converter.constants._
import scala.collection.mutable.ListBuffer

object Client extends App {
  println("Conversion Started")
  val flfilepath = args(0)
  val csvFilePath= args(1)
  val parquetFilePath = args(2)
  val flSchema = args(3)
  
  val funcObj = new functions
  var schemaMetaMap = scala.collection.mutable.Map[String, String]()

  val exceldf = funcObj.createExceldf(flSchema, funcObj.getSparkSession(), "Sheet1")
  exceldf.createOrReplaceTempView("schemaInfo")

  val sqlDF = funcObj.getSparkSession().sql(sql.getFieldName + "schemaInfo").select("Field").rdd.map(r => r(0)).collect()
  val metaSchema = funcObj.getSchemaMetadat(sqlDF)
  funcObj.fldataparser(flfilepath, metaSchema)

}