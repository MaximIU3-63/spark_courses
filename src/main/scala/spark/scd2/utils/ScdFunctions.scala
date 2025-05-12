package spark.scd2.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, current_date, current_timestamp, lit, sha1}
import org.apache.spark.sql.types.StructType

object ScdFunctions {
  /** Детектирование изменений с использованием хеширования */
  def detectChanges(
                             existingDF: DataFrame, 
                             incomingDF: DataFrame,
                             primaryKeyColumns: Seq[String],
                             sensitiveKeysColumns: Seq[String]
                           ): DataFrame = {

    val hashColumns = (primaryKeyColumns ++ sensitiveKeysColumns).map(col)

    val existingWithHashDF = existingDF.
      select(primaryKeyColumns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.HASH_COLUMN): _*)

    val incomingWithHashDF = incomingDF.
      select(incomingDF.columns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.HASH_COLUMN): _*)

    incomingWithHashDF.alias("incoming").
      join(existingWithHashDF.alias("existing"), Seq(Constants.HASH_COLUMN), "left_anti").
      select(incomingDF.columns.map(col): _*)
  }

  /** Нахождение активных записей без изменений */
  def getUnchangedActiveRecords(
                                         existingActiveDF: DataFrame, 
                                         changesDF: DataFrame,
                                         primaryKeyColumns: Seq[String]
                                       ): DataFrame = {
    existingActiveDF.join(
      changesDF.select(primaryKeyColumns.map(col): _*),
      primaryKeyColumns,
      "left_anti"
    )
  }

  /** Закрытие устаревших записей */
  def expireOldRecords(
                                existingActiveDF: DataFrame, 
                                changesDF: DataFrame,
                                primaryKeyColumns: Seq[String],
                                effectiveDateTo: String,
                                partitionColName: String,
                                sysDateColumnName: String,
                                sysDateDttmColumnName: String
                              ): DataFrame = {
    existingActiveDF.
      join(changesDF, primaryKeyColumns, "left_semi").
      withColumn(effectiveDateTo, DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)).
      withColumn(partitionColName, lit(Inactive.value)).
      withColumn(sysDateColumnName, lit(DateFormat.formatDateColumn(current_date(), ISO_8601))).
      withColumn(sysDateDttmColumnName, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
  }

  /** Формирование дата фрейма с новыми и обновленными данными */
  def prepareUpsertRecords(
                                    changesDF: DataFrame,
                                    effectiveDateFrom: String,
                                    effectiveDateTo: String,
                                    partitionColName: String,
                                    sysDateColumnName: String,
                                    sysDateDttmColumnName: String
                                  ): DataFrame = {
    changesDF
      .withColumn(effectiveDateFrom, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
      .withColumn(effectiveDateTo, lit(Dates.closeDateDttmValue))
      .withColumn(partitionColName, lit(Active.value))
      .withColumn(sysDateColumnName, lit(DateFormat.formatDateColumn(current_date(), ISO_8601)))
      .withColumn(sysDateDttmColumnName, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
  }

  /** Объединение дата фреймов */
  def combineDataFrames(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_ unionByName _)

  /** Приведение полей дата фрейма к целевому маппингу и типу данных */
  def castColumnsToTargetSchema(prioritySchema: StructType, df: DataFrame): DataFrame = {
    // Получаем схему целевого исходного датафрейма
    val existingSchema = prioritySchema.fields.map(
      f => (f.name, f.dataType)
    ).toMap

    // Преобразуем схему конечного датафрейма к целевому
    val incomingCastedColumns = df.columns.map {
      colName => existingSchema.get(colName) match {
        case Some(colType) => df(colName).cast(colType).as(colName)
        case None => df(colName)
      }
    }

    df.
      select(incomingCastedColumns: _*).
      select(prioritySchema.map(c => col(c.name)): _*)
  }
}
