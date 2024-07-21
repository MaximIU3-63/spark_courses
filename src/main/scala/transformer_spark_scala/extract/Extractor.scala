package transformer_spark_scala.extract

import org.apache.spark.sql.{Column, DataFrame => DF, SparkSession}

object Extractor {
  //  def apply(tableName: String, columns: Seq[Column])/*(implicit spark: SparkSession)*/: Unit = {
  //    new Extractor(tableName, columns, filterCond = null).
  //      extract()
  //  }
  //
  //  def apply(tableName: String, columns: Seq[Column], filterCond: Column)/*(implicit spark: SparkSession)*/: Unit = {
  //    new Extractor(tableName, columns, filterCond).
  //      extract()
  //  }

  case class ColumnsConf(columns: Seq[Column], filterCond: Column = null) {
    require(columns.nonEmpty, "Columns must be specified.")
  }

  case class TableConfig(tableName: String, colsConf: ColumnsConf) {
    require(tableName.nonEmpty, "Table name must be specified.")
  }
}

case class Extractor(spark: SparkSession, config: Extractor.TableConfig)/*(implicit spark: SparkSession)*/ {
  def extract(): Unit = {
    require(spark.catalog.tableExists(config.tableName), s"Table ${config.tableName} does not exists.")
    if(config.colsConf.filterCond == null) {
      println(
        s"""
           |spark.table(${config.tableName}).
           |        select(
           |          columns: _*
           |        )
           |""".stripMargin)

    } else {
      println(
        s"""
           |spark.table(${config.tableName}).
           |        filter(${config.colsConf.filterCond}).
           |        select(
           |          columns: _*
           |        )
           |""".stripMargin)
    }
  }
}
