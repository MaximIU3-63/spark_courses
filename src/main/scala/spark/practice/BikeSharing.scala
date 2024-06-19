package spark.practice

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders}
import spark.utils.ContextBuilder

object BikeSharing extends App with ContextBuilder {

  override val appName: String = "BikeSharing"

  val fileLocation: String = "src/main/resources/bike_sharing.csv"

  case class Sharing(date: String,
                     rented_bike_count: Int,
                     hour: Int,
                     temperature: String,
                     humidity: Int,
                     wind_speed: Double,
                     visibility: Int,
                     dew_point_temperature: Double,
                     solar_radiation: Double,
                     rainfall: Int,
                     snowfall: Int,
                     seasons: String,
                     holiday: String,
                     functioning_day: String)

  val bikeSharingSchema: StructType = Encoders.product[Sharing].schema

  val bikeSharingDF: DataFrame = spark.read.
    format("csv").
    schema(bikeSharingSchema).
    options(
      Map(
        "header" -> "true",
        "sep" -> ",",
        "multiLine" -> "true",
        "path" -> fileLocation
      )
    ).
    load()

  def withWorkDay(colName: String)(df: DataFrame): DataFrame = {
    val columns = Seq(col("holiday"), col("functioning_day"))
    val isHoliday = col("holiday") === lit("Holiday") && col("functioning_day") === lit("No")

    df.
      select(columns: _*).
      distinct().
      withColumn(colName, when(isHoliday, 0).otherwise(1)).
      select(columns.appended(col(colName)): _*)
  }

  val iswordayDF = bikeSharingDF.
    transform(withWorkDay("is_workday"))

  iswordayDF.show()

  spark.close()
}