package work

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, array_contains, col, collect_set, current_date, date_add, last_day, lit, rand, row_number, size, to_date, to_timestamp, when}
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import spark.utils.ContextBuilder

import scala.util.{Failure, Random, Success, Try}

object Traffic extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName
  private val trFileLocation: String = "src/main/resources/traffic.csv"
  private val expenseFileLocation: String = "src/main/resources/expense_h.csv"

  spark.sparkContext.setLogLevel("WARN")

  private def read(file: String): DataFrame = {
    spark.read.
      options(
        Map(
          "header" -> "true",
          "inferSchema" -> "false"
        )
      ).
      csv(file)
  }

  private val trafficDF = read(trFileLocation)

  private val expenseDF = read(expenseFileLocation)

  private val expenseFiltered = expenseDF.
    withColumn("array_report_dt", collect_set(col("report_dt")).over(Window.partitionBy(to_date(col("src_load_dttm"))))).
    withColumn("rn", when(
      array_contains(col("array_report_dt"), last_day(current_date()).cast("string")) &&
      size(col("array_report_dt")) === 1, lit(0)).otherwise(lit(1)))

  expenseFiltered.orderBy(col("src_load_dttm").desc).show(43, truncate = false)

  val resultExpenseDF = expenseFiltered.
    filter(col("rn") === lit(1) && col("report_dt") === last_day(add_months(current_date(), -1)))

  resultExpenseDF.orderBy(col("src_load_dttm").desc).show(43, truncate = false)


  val actualTrafficDF: DataFrame = {
    if (expenseDF.sample(fraction = 0.01).count() == 0L) {
      println(s"The historical data mart with traffic by expenses is empty.")
      trafficDF
    } else {
      println(
        s"""The historical data mart with traffic by expenses is not empty.
           |Looking for data that requires calculation
           |""".stripMargin)

      val top3reportDt: List[String] = expenseDF.
        select(
          col("report_dt")
        ).
        distinct().
        orderBy(col("report_dt").desc).
        take(6).
        toList.
        map(_.getString(0))

      println(s"top3reportDt - ${top3reportDt.mkString(", ")}")

      val top3sendDt: List[String] = trafficDF.
        select(
          last_day(col("send_dt")).cast("string").as("send_dt")
        ).
        distinct().
        orderBy(col("send_dt").desc).
        take(6).
        toList.
        map(_.getString(0))

      println(s"top3sendDt - ${top3sendDt.mkString(", ")}")

      val rawDates: List[String] = top3sendDt.diff(top3reportDt)

      println(s"rawDates - ${rawDates.mkString(", ")}")

      trafficDF.
        filter(
          last_day(col("send_dt")).isin(rawDates: _*)
        )
    }
  }

  spark.close
}
