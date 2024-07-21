package transformer_spark_scala.extract

import spark.utils.ContextBuilder
import transformer_spark_scala.extract.columns.KantorColumns

object Main extends App with ContextBuilder {
  override val appName: String = ""

  val rraColsConfigs = Extractor.ColumnsConf(KantorColumns.rraCols)
  val rraConfigs = Extractor.TableConfig("prod_kantor.rra", rraColsConfigs)

  val rraDF = Extractor(spark, rraConfigs).extract()

  spark.close()
}
