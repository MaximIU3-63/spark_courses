package spark.utils

import org.apache.spark.sql.SparkSession

trait ContextBuilder {

  private def createSparkSession(appName: String) = {
    SparkSession.
      builder().
      master("local[*]").
      appName(appName).
      getOrCreate()
  }

  val appName: String

  lazy val spark: SparkSession = createSparkSession(appName)
}
