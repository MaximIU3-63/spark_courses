package transformer_spark_scala.load

import org.apache.spark.sql.DataFrame

trait Loader {
  def load(): Unit
}
