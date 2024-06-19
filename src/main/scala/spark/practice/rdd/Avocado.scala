package spark.practice.rdd

import org.apache.spark.rdd.RDD
import spark.utils.ContextBuilder

import java.text.SimpleDateFormat
import java.util.Date
import scala.io.Source
import scala.util.Try

object DatesDict {
  val WithDash = "yyyy-MM-dd"
  val DefaultDate = "1900-01-01"
  val Date = "2018-02-11"

  val MonthDescriptionMap: Map[String, String] = Map(
    ("00", "UNKNOWN"),
    ("01", "JANUARY"),
    ("02", "FEBRUARY"),
    ("03", "MARCH"),
    ("04", "APRIL"),
    ("05", "MAY"),
    ("06", "JUNE"),
    ("07", "JULY"),
    ("08", "AUGUST"),
    ("09", "SEPTEMBER"),
    ("10", "OCTOBER"),
    ("11", "NOVEMBER"),
    ("12", "DECEMBER")
  )
}

object Avocado extends App with ContextBuilder{

  override val appName: String = this.getClass.getSimpleName

  val sc = spark.sparkContext

  sc.setLogLevel("WARN")

  val fileLocation: String = "src/main/resources/avocado.csv"

  case class Avocado(
                      id: Int,
                      date: String,
                      avgPrice: Double,
                      volume: Double,
                      year: String,
                      region: String
                    )

  def readFromSource(fileLocation: String): List[Avocado] = {
    val bufferedSource = Source.fromFile(fileLocation)
    bufferedSource.
      getLines.
      drop(1).
      map(line => line.split(",")).
      filter(line => line.nonEmpty).
      map(value => {
        Avocado(
          value(0).toInt,
          value(1),
          value(2).toDouble,
          value(3).toDouble,
          value(12),
          value(13),
        )
      }).toList
  }

  val avocadoRDD: RDD[Avocado] = sc.parallelize(readFromSource(fileLocation))

  //Расчет количества уникальных регионов (region), для которых представлена статистика
  val uniqRegions: Long = avocadoRDD.map(_.region).
    distinct().count()

  println(s"Количество уникальных регионов из статистики: $uniqRegions\n")

  //Записи о продажах авокадо, сделанные после 2018-02-11
  def parseDate(date: String): Date = {
    Try{
      new SimpleDateFormat(DatesDict.WithDash).parse(date)
    }.toEither match {
      case Left(_) => new SimpleDateFormat(DatesDict.WithDash).
        parse(DatesDict.DefaultDate)
      case Right(date) => date
    }
  }

  def greaterThan(date: String, constDate: String): Boolean = {
    parseDate(date).
      after(parseDate(constDate))
  }

  val salesAfterDateRDD: RDD[Avocado] = avocadoRDD.
    filter(row => greaterThan(row.date, DatesDict.Date))

  println("Записи из статистики, сделанные после 2018-02-11:")
  salesAfterDateRDD.foreach(println)
  println()

  //Информация о месяце, который чаще всего представлен в статистике
  def splitDate(date: String): String = {
    Try {
      date.split("-")(1)
    }.toEither match {
      case Left(_) => "00"
      case Right(month) => month
    }
  }

  val mostFrequantMonthRDD = DatesDict.MonthDescriptionMap.
    getOrElse(avocadoRDD.
      map(row => splitDate(row.date)).
      groupBy(identity).
      map(row => (row._1, row._2.size)).
      max.
      _1, "unknown")

  println(s"Наиболее часто встречающийся месяц в статистике - $mostFrequantMonthRDD\n")

  //максимальное и минимальное значение avgPrice
  val avgPrice: RDD[Double] = avocadoRDD.map(_.avgPrice)

  val avgPriceMax: Double = avgPrice.max()
  val avgPriceMin: Double = avgPrice.min()

  println(s"Максимальная средняя цена - $avgPriceMax")
  println(s"Минимальная средняя цена - $avgPriceMin\n")

  //средний объем продаж (volume) для каждого региона (region)
  val avgVolumeByRegion: RDD[(String, Double)] = avocadoRDD.
    map(row => (row.region, row.volume)).
    groupByKey().
    map(row => (row._1, row._2.sum / row._2.size))

  println("Средние объем продаж по регионам:")
  avgVolumeByRegion.foreach(println)

  spark.close()
}
