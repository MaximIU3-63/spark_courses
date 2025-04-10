package spark.scd2.utils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}

/** Объект с описаниями правил отбора актуальных и неактуальных записей */
object Filters {

  /** Фильтрует записи по полю партиции. */
  def filterByPartitionValue(
                              df: DataFrame,
                              column: String,
                              value: Int
                            ): DataFrame = df.filter(col(column) === value)
}
