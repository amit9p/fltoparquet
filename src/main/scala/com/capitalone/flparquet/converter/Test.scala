package com.capitalone.flparquet.converter

import scala.io.Source
import com.capitalone.flparquet.converter.common._

object Test extends App {

  val obj = new functions()
  val spark = obj.getSparkSession()
  import spark.implicits._

  val peopleDF = spark.sparkContext
    .textFile("/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/fixedlength.txt")

  val x = peopleDF.count()

  val parquetFileDF = spark.read.parquet("/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/csvFile.parquet")
  parquetFileDF.show

}