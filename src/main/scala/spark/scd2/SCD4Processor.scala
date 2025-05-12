package spark.scd2

import org.apache.spark.sql.functions.{current_date, current_timestamp, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import spark.scd2.utils.{BaseTechnicalColumns, DateFormat, ISO_8601, ISO_8601_EXTENDED, Messages, ScdFunctions}

/**
 * Кейс класс, наследующий трейт с базовыми техническими полями и определяющих их наименование
 * @param sysDateColumnName - наименования системного поля в формате ISO-8601
 * @param sysDateDttmColumnName - наименования системного поля в формате ISO-8601_EXTENDED
 */
case class CurrentTechnicalColumns(
                                    sysDateColumnName: String = "sysdate_dt",
                                    sysDateDttmColumnName: String = "sysdate_dttm"
                                  ) extends BaseTechnicalColumns

/**
 * Конфигуратор для SCD4
 * @param primaryKeyColumns - массив primary полей для связи таблиц.
 * @param sensitiveKeysColumns - массив чувствительных к изменению полей.
 * @param technicalColumn - конфигуратор технических полей.
 */
case class SCD4Config(
                       primaryKeyColumns: Seq[String],
                       sensitiveKeysColumns: Seq[String],
                       technicalColumn: BaseTechnicalColumns
                     ) {
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("primaryKeyColumns"))
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("sensitiveKeysColumns"))
  require(primaryKeyColumns.intersect(sensitiveKeysColumns).isEmpty, "Primary and sensitive columns must not overlap")
}

class SCD4Processor(config: SCD4Config) {

  /** Основной метод обработки SCD2 */
  def process(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {
    // 1. Формирование дата фрейма строк, где есть изменения
    val changesDF = ScdFunctions.detectChanges(
        existingDF,
        incomingDF,
        config.primaryKeyColumns,
        config.sensitiveKeysColumns).
      persist(StorageLevel.MEMORY_AND_DISK)

    val existingActiveSchema: StructType = existingDF.schema

    val scd4DF = ScdFunctions.castColumnsToTargetSchema(
      existingActiveSchema,
      changesDF.
        withColumn(config.technicalColumn.sysDateColumnName, lit(DateFormat.formatDateColumn(current_date(), ISO_8601))).
        withColumn(config.technicalColumn.sysDateDttmColumnName, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
    )

    // Возврат результирующего датафрейма
    scd4DF
  }
}
