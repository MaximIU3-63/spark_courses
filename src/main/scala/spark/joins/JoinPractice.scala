package spark.joins

import spark.utils.ContextBuilder

object JoinPractice extends App with ContextBuilder {

  override val appName: String = "JoinPractice"

  spark.sparkContext.setLogLevel("WARN")

  val valuesDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_link_value.csv")

  val tagsDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_links.csv")

  val joinCondition = valuesDF.col("id") === tagsDF.col("key")

  // inner используется по умолчанию, поэтому его можно не указывать
  val innerJoinDF = tagsDF.join(valuesDF, joinCondition, "inner")
  innerJoinDF.show()

  val fullOuterDF = tagsDF.join(valuesDF, joinCondition, "outer")
  fullOuterDF.show()

  val leftOuterDF = tagsDF.join(valuesDF, joinCondition, "left_outer")
  leftOuterDF.show()

  val rightOuterDF = tagsDF.join(valuesDF, joinCondition, "right_outer")
  rightOuterDF.show()

  val leftSemiDF = tagsDF.join(valuesDF, joinCondition, "left_semi")
  leftSemiDF.show()

  spark.close()

}
