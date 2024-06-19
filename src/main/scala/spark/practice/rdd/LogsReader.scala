package spark.practice.rdd

import org.apache.spark.rdd.RDD
import spark.utils.ContextBuilder

import java.time.{Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.Try

object DefaultValue {
  val StringTypeDefaultValue: String = "na"
  val IntTypeDefaultValue: Int = -1
  val LongTypeDefaultValue: Long = -1L
  val ResponseCode: Int = 404
}

object DatePattern {
  val WithDash = "yyyy-MM-dd"
}

object LogsReader extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  val sc = spark.sparkContext

  sc.setLogLevel("WARN")

  private val fileLocation: String = "src/main/resources/logs_data.csv"

  case class Logs(
                   id: Int,
                   host: String,
                   time: Long,
                   method: String,
                   url: String,
                   response: Int,
                   bytes: Int
                 )

  def readCsvAsRDD(fileLocation: String): RDD[Logs] = {
    sc.textFile(fileLocation).
      map(line => line.split(",")).
      filter(value => value(0).nonEmpty).
      map(value =>
        Logs(
          Try(value(0).toInt).toOption.
            getOrElse(DefaultValue.IntTypeDefaultValue),
          Try(value(1)).toOption.
            getOrElse(DefaultValue.StringTypeDefaultValue),
          Try(value(2).toLong).toOption.
            getOrElse(DefaultValue.LongTypeDefaultValue),
          Try(value(3)).toOption.
            getOrElse(DefaultValue.StringTypeDefaultValue),
          Try(value(4)).toOption.
            getOrElse(DefaultValue.StringTypeDefaultValue),
          Try(value(5).toInt).toOption.
            getOrElse(DefaultValue.IntTypeDefaultValue),
          Try(value(6).toInt).toOption.
            getOrElse(DefaultValue.IntTypeDefaultValue)
        ))
  }

  def getIncorrectlyReadLines(rdd: RDD[Logs]): Long = {
    rdd.
      filter(row => {
        row.id == DefaultValue.IntTypeDefaultValue ||
          row.host == DefaultValue.StringTypeDefaultValue ||
          row.time == DefaultValue.LongTypeDefaultValue ||
          row.method == DefaultValue.StringTypeDefaultValue ||
          row.url == DefaultValue.StringTypeDefaultValue ||
          row.response == DefaultValue.IntTypeDefaultValue ||
          row.bytes == DefaultValue.IntTypeDefaultValue
      }).
      count()
  }

  def getResponseStatistics(rdd: RDD[Logs]): RDD[(Int, Int)] = {
    rdd.
      filter(value => value.response != DefaultValue.IntTypeDefaultValue).
      map(value => (value.response, 1)).
      reduceByKey(_ + _)
  }

  def extractBytesRDD(rdd: RDD[Logs]): RDD[Int] = {
    rdd.
      filter(row => row.bytes != DefaultValue.IntTypeDefaultValue).
      map(_.bytes)
  }

  def getBytesSum(rddBytes: RDD[Int]): Int = {
    rddBytes.
      sum().
      toInt
  }

  def getBytesMaxValue(rddBytes: RDD[Int]): Int = {
    rddBytes.
      max()
  }

  def getBytesMinValue(rddBytes: RDD[Int]): Int = {
    rddBytes.
      min()
  }

  def getBytesAvgValue(sum: Int, count: Long): Double = {
    sum / count
  }

  def getUniqHostCount(rdd: RDD[Logs]): Long = {
    rdd.
      map(_.host).
      distinct().
      count()
  }

  def getTopThreeHostsByCount(rdd: RDD[Logs]): Array[(String, Int)] = {
    rdd.
      map(row => (row.host, 1)).
      reduceByKey(_ + _).
      top(3)(Ordering.by[(String, Int), Int](_._2))
  }

  def getDayOfWeekFromUnix(epochTimeMillis: Long): String = {
    Try {
      val formatter = DateTimeFormatter.ofPattern(DatePattern.WithDash)
      val instant = Instant.ofEpochMilli(epochTimeMillis * 1000)
      val zoneId = ZoneId.systemDefault()
      val localDate = instant.atZone(zoneId).toLocalDate
      val formattedDate = localDate.format(formatter)
      val dayOfWeek = LocalDate.parse(formattedDate, formatter).getDayOfWeek.toString
      dayOfWeek
    }.toEither match {
      case Left(_) => "na"
      case Right(value) => value
    }
  }

  def getTop3DaysByResponse(responseCode: Int, rdd: RDD[Logs]): Array[(String, Int)] = {
    rdd.
      filter(row => row.response == responseCode).
      map(row => (getDayOfWeekFromUnix(row.time), 1)).
      reduceByKey(_ + _).
      top(3)(Ordering.by[(String, Int), Int](_._2))
  }


  val logsRDD: RDD[Logs] = readCsvAsRDD(fileLocation)

  //статистика по успешно и неуспешно считанным строчкам
  val incorrectlyReadLines: Long = getIncorrectlyReadLines(logsRDD)

  val correctlyReadLines: Long = logsRDD.count() - incorrectlyReadLines

  println(
    s"""
       |Statistics on read lines:
       |\tincorrectly read lines count - $incorrectlyReadLines
       |\tcorrectly read lines count - $correctlyReadLines
       |""".stripMargin)

  //количество записей для каждого кода ответа (response)
  val responseStatisticsRDD: RDD[(Int, Int)] = getResponseStatistics(logsRDD)

  println("Response statistics:")
  responseStatisticsRDD.
    foreach(response => {
      println(s"\tresponse: ${response._1}, number of records: ${response._2}")
    })

  //статистика по размеру ответа (bytes): сколько всего байт было отправлено (сумма всех значений), среднее значение, максимальное и минимальное значение.
  val bytesRDD = extractBytesRDD(logsRDD)

  val sumBytesValue = getBytesSum(bytesRDD)
  val maxBytesValue = getBytesMaxValue(bytesRDD)
  val minBytesValue = getBytesMinValue(bytesRDD)

  val bytesRowCnt = bytesRDD.count()
  val avgBytesValue = getBytesAvgValue(sumBytesValue, bytesRowCnt)

  println(
    s"""
       |Response statistics by bytes:
       |\tmax bytes value - $maxBytesValue,
       |\tmin bytes value - $minBytesValue,
       |\tsum bytes value - $sumBytesValue,
       |\tavg bytes value - $avgBytesValue,
       |""".stripMargin)

  //количество уникальных хостов (host)
  val uniqHostsCount: Long = getUniqHostCount(logsRDD)

  println(s"Unique number of hosts - $uniqHostsCount\n")

  //Топ-3 наиболее часто встречающихся хостов (host).
  val top3Hosts = getTopThreeHostsByCount(logsRDD)

  println("Top 3 hosts by number of records:")
  top3Hosts.foreach(stat => {
    println(s"\thost: ${stat._1}, number of records: ${stat._2}")
  })

  //Дни недели (понедельник, вторник и тд), когда чаще всего выдавался ответ (response) 404.
  val top3DaysByResponse = getTop3DaysByResponse(DefaultValue.ResponseCode, logsRDD)

  println("\nTop 3 days by number of records:")
  top3DaysByResponse.foreach(stat => {
    println(s"\tday - ${stat._1}, number of records - ${stat._2}")
  })

  spark.close()
}
