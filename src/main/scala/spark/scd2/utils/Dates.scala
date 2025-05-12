package spark.scd2.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.date_format

/** Объект, содержащий значение закрытой даты */
private[scd2] object Dates {
  lazy val closeDateDttmValue: String = "5999-12-31 00:00:00.000" // Значение закрытой даты
}

/** Трейт для описания форматов даты */
sealed trait DateFormatter

/** Формат даты ISO_8601 (год-месяц-день) */
private[scd2] case object ISO_8601 extends DateFormatter {
  override implicit def toString: String = "yyyy-MM-dd" // Формат даты
}

/** Расширенный формат даты ISO_8601 (год-месяц-день часы:минуты:секунды.миллисекунды) */
private[scd2] case object ISO_8601_EXTENDED extends DateFormatter {
  override implicit def toString: String = "yyyy-MM-dd HH:mm:ss.SSS" // Формат даты
}

/** Объект, реализующий метод преобразования даты в указанный формат */
private[scd2] object DateFormat {
  /**
   * Преобразует колонку с датой в указанный формат
   *
   * @param column Колонка с датой
   * @param format Формат даты
   * @return Колонка с преобразованной датой
   */
  def formatDateColumn(column: Column, format: DateFormatter): Column = {
    date_format(column, format.toString)
  }
}
