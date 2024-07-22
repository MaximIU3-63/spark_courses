package transformer_spark_scala.load
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object ErrorMessages {
  def unsupportedFormat(format: String): String = s"unsupported output format $format"
  def unspecifiedValue(value: String): String = s"$value must be specified."
}

object DataFrameWriter {
  case class Config(
                     tableName: String,
                     saveMode: SaveMode,
                     outputFormat: String,
                     partitionCols: Seq[Column] = null
                   ) {
    require(tableName.nonEmpty, ErrorMessages.unspecifiedValue(tableName))
    require(Seq("parquet").contains(outputFormat), ErrorMessages.unsupportedFormat(outputFormat))
  }
}

case class DataFrameTableWriter(df: DataFrame, conf: DataFrameWriter.Config) extends Loader {
  override def load(): Unit = {
    if(conf.partitionCols.isEmpty) {
      df.
        repartition(1).
        write.
        mode(conf.saveMode).
        format(conf.outputFormat).
        insertInto(conf.tableName)
    } else {
      df.
        repartition(1).
        write.
        mode(conf.saveMode).
        partitionBy(conf.partitionCols.mkString(", ")).
        format(conf.outputFormat).
        insertInto(conf.tableName)
    }
  }
}
