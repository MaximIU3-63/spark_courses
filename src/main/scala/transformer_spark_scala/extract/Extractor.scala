package transformer_spark_scala.extract

import org.apache.spark.sql.{Column, SparkSession, DataFrame => DF}

object ErrorMessages {
  def unspecifiedValue(value: String) = s"$value must be specified."
}

object Extractor {
  case class TableConfig(tableName: String, columns: Seq[Column], filterCond: Column = null) {
    require(tableName.nonEmpty, ErrorMessages.unspecifiedValue("Table name"))
    require(columns.nonEmpty, ErrorMessages.unspecifiedValue("Columns"))
  }
}

case class Extractor(config: Extractor.TableConfig)(implicit spark: SparkSession) {
  def extract(): DF = {

    require(spark.catalog.tableExists(config.tableName), s"Table ${config.tableName} does not exists.")

    if(config.filterCond == null) {
      println(
        s"""
           |spark.table(${config.tableName}).
           |        select(
           |          columns: _*
           |        )
           |""".stripMargin)

      spark.emptyDataFrame
    } else {
      println(
        s"""
           |spark.table(${config.tableName}).
           |        filter(${config.filterCond}).
           |        select(
           |          columns: _*
           |        )
           |""".stripMargin)
      spark.emptyDataFrame
    }
  }
}
