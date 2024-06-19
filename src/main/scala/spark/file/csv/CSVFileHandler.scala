package spark.file.csv

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode}
import spark.utils.ContextBuilder

object CSVFileHandler extends App with ContextBuilder {

  override val appName: String = "CSVFileHandler"

  val fileLocation: String = "src/main/resources/movies_on_netflix.csv"

  case class MoviesSchema(id: Int,
                          show_id: String,
                          `type`: String,
                          title: String,
                          director: String,
                          cast: String,
                          country: String,
                          date_added: String,
                          release_year: String,
                          rating: String,
                          duration: Int,
                          listed_in: String,
                          description: String,
                          year_added: String,
                          month_added: Double,
                          season_count: Int
                         )

  val moviesSchema: StructType = Encoders.product[MoviesSchema].schema

  val moviesDF: DataFrame = spark.read.
    format("csv").
    schema(moviesSchema).
    options(
      Map(
        "header" -> "true",
        "sep" -> ",",
        "multiLine" -> "true",
        "path" -> fileLocation
      )
    ).
    load().
    withColumn("month_added", col("month_added").cast("int"))

  try {
    moviesDF.
      write.
      mode(SaveMode.Overwrite).
      parquet("src/main/resources/data")
  } catch {
    case ex: Throwable => println(ex.printStackTrace())
  } finally {
    spark.close()
  }
}
