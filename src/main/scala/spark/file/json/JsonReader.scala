package spark.file.json

import org.apache.spark.sql.SparkSession

object JsonReader extends App {
  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local")
    .getOrCreate()

  val json = spark.read.format("file").
    options(Map("inferSchema" -> "true")).
    load("src/main/resources/restaurant_ex.json")

  json.printSchema()
}
