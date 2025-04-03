package transformer_spark_scala

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col
import spark.utils.ContextBuilder
import transformer_spark_scala.extract.Extractor
import transformer_spark_scala.load.{DataFrameWriter, DataFrameTableWriter}

object Main extends App with ContextBuilder {
  override val appName: String = this.getClass.getSimpleName

  //EXTRACT
  private val rraExtractConf = Extractor.TableConfig("prod_kantor.rra", Seq(col("rra")))
  val rraDF: DataFrame = Extractor(rraExtractConf).extract()

  //TRANSFORM
  ???

  //LOAD
  val rraWriteConfig = DataFrameWriter.Config("rra_1", SaveMode.Overwrite, "parquet")
  DataFrameTableWriter(rraDF, rraWriteConfig)

  spark.close()
}
