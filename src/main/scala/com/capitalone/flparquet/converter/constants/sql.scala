package com.capitalone.flparquet.converter.constants

object sql {
  
  val getFieldName = "select Field from "
  val getStartLength = "SELECT  StartLength FROM schemaInfo WHERE Field ="
  val getEndLength = "SELECT  EndLength FROM schemaInfo WHERE Field ="
}