package transformer_spark_scala

import spark.utils.ContextBuilder

object Main extends App with ContextBuilder {
  override val appName: String = this.getClass.getSimpleName


  spark.close()
}
