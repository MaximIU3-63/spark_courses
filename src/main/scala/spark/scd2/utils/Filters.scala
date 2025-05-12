package spark.scd2.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** Объект с описаниями правил отбора актуальных и неактуальных записей */
object Filters {

  /**
   * Фильтрует записи по значению в указанной колонке партиции.
   *
   * @param df DataFrame, который нужно отфильтровать.
   * @param column Имя колонки, по которой выполняется фильтрация.
   * @param value Значение, по которому производится фильтрация.
   * @return DataFrame с отфильтрованными записями.
   */
  def filterByPartitionValue(
                              df: DataFrame,
                              column: String,
                              value: Int
                            ): DataFrame = df.filter(col(column) === value)
}
