package spark.practice.datasets

import java.text.SimpleDateFormat
import org.apache.spark.sql.{Dataset, Encoders}
import spark.utils.ContextBuilder

import java.time.Instant
import java.util.Locale
import scala.util.Try

object DateFormat {
  val WithDash = "yyyy-MM-dd"
  val WithSpace = "yyyy MM dd"
  val WithMonthNum = "yyyy MMM dd"
}

object Cars extends App with ContextBuilder {
  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  case class Car(
                  id: Int,
                  price: Int,
                  brand: String,
                  `type`: String,
                  mileage: Option[Double],
                  color: String,
                  dateOfPurchase: String
                )

  case class CarAdditionalInfo(
                          id: Int,
                          price: Int,
                          brand: String,
                          `type`: String,
                          mileage: Option[Double],
                          color: String,
                          dateOfPurchase: String,
                          avgMiles: Double,
                          yearsSincePurchase: Int
                        )

  import spark.implicits._

  val carsSchema = Encoders.product[Car].schema

  val carsDS: Dataset[Car] = spark.read.
    options(
      Map(
        "header" -> "true",
        "path" -> "src/main/resources/cars.csv"
      )
    ).
    schema(carsSchema).
    csv().
    as[Car]

  def toDate(date: String, dateFormat: String): Try[Long] = {
    Try {
      val format = new SimpleDateFormat(dateFormat, Locale.ENGLISH)
      format.parse(date).getTime
    }
  }

  def calculateAvgMiles(car: Dataset[Car]): Double = {
    val (miles, carsCount) = car.
      map(row => (row.mileage.getOrElse(0.0), 1)).
      reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    miles / carsCount
  }

  def withCarsAgeAndAvgMiles(avgMiles: Double, currentDate: Long)(ds: Dataset[Car]): Dataset[CarAdditionalInfo] = {
    ds.map(rec => {
      val purchaseDate = toDate(rec.dateOfPurchase, DateFormat.WithDash).
        orElse(toDate(rec.dateOfPurchase, DateFormat.WithSpace)).
        orElse(toDate(rec.dateOfPurchase, DateFormat.WithMonthNum)).
        getOrElse(0L)

      val age = (currentDate - purchaseDate) / (1000 * 60 * 60 * 24) / 365

      CarAdditionalInfo(
        rec.id,
        rec.price,
        rec.brand,
        rec.`type`,
        rec.mileage,
        rec.color,
        rec.dateOfPurchase,
        avgMiles,
        age.toInt
      )
    })
  }

  val avgMiles: Double = calculateAvgMiles(carsDS)

  val currentDate = Instant.now().toEpochMilli

  val carAdditionalInfoDS = carsDS.
    transform(withCarsAgeAndAvgMiles(avgMiles, currentDate))

  carAdditionalInfoDS.show()

  spark.close()
}