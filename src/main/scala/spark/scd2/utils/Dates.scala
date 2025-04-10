package spark.scd2.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.date_format

private[scd2] object Dates {
  lazy val closeDateDttmValue: String = "5999-12-31 00:00:00.000"
}

sealed trait DateFormatter

/** Описание формата даты ISO_8601 */
private[scd2] case object ISO_8601 extends DateFormatter {
  override implicit def toString: String = "yyyy-MM-dd"
}

/** Описание формата даты ISO_8601 */
private[scd2] case object ISO_8601_EXTENDED extends DateFormatter {
  override implicit def toString: String = "yyyy-MM-dd HH:mm:ss.SSS"
}

/** Объект, реализующий метод преобразования даты к нужному формату */
private[scd2] object DateFormat {
  def formatDateColumn(column: Column, format: DateFormatter): Column = {
    date_format(column, format.toString)
  }
}
