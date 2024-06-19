package spark.cols

import org.apache.spark.sql.functions.{col, lit, max, min, to_date, when}
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

  val bikeSharingDataDF: DataFrame = bikeSharingDF.
    select(
      col("hour"),
      col("temperature"),
      col("humidity"),
      col("wind_speed")
    )

  bikeSharingDataDF.show(3)

  val cnt = bikeSharingDF.
    select(
      col("rented_bike_count"),
      col("temperature")
    ).filter(col("rented_bike_count") === lit(254) && col("temperature").cast("double") > lit(0.0)).
    count()

  println(cnt)

  val isHoliday = col("holiday") === lit("Holiday") && col("functioning_day") === lit("No")

  val iswordayDF = bikeSharingDF.
    select(
      col("holiday"),
      col("functioning_day")
    ).distinct().
    withColumn("is_workday", when(isHoliday, 0).otherwise(1))

  val temperatureByDateDF = bikeSharingDF.
    select(
      col("date"),
      col("temperature").cast("double")
    ).
    groupBy(col("date")).
    agg(
      min(col("temperature")).as("min_temp"),
      max(col("temperature")).as("max_temp")
    )
  temperatureByDateDF.show()

  spark.close()
}
