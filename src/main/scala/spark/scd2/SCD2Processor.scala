package spark.scd2

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.scd2.utils.{Active, BaseTechnicalColumns, CacheManager, Constants, DateFormat, Dates, Filters, ISO_8601, ISO_8601_EXTENDED, Inactive, Messages, SCD2Defaults, SCD2PartitioningConfig, ScdFunctions}

import scala.util.{Failure, Success}

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
 * Конфигуратор для SCD2
 * @param primaryKeyColumns - массив primary полей для связи таблиц.
 * @param sensitiveKeysColumns - массив чувствительных к изменению полей.
 * @param effectiveDateFrom - наименование поля 'дата открытия' строки.
 * @param effectiveDateTo - наименование поля 'даты закрытия' строки.
 * @param technicalColumn - конфигуратор технических полей.
 * @param partitionConfig - конфигуратор для поля партицирования: По умолчанию описан в SCD2Defaults.DefaultPartition
 */
case class SCD4Config(
                       primaryKeyColumns: Seq[String],
                       sensitiveKeysColumns: Seq[String],
                       effectiveDateFrom: String,
                       effectiveDateTo: String,
                       technicalColumn: BaseTechnicalColumns,
                       partitionConfig: SCD2PartitioningConfig = SCD2Defaults.DefaultPartition
                     ) {
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("primaryKeyColumns"))
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("sensitiveKeysColumns"))
  require(primaryKeyColumns.intersect(sensitiveKeysColumns).isEmpty, "Primary and sensitive columns must not overlap")
}

class SCD2Processor(config: SCD4Config) {
  /** Основной метод обработки SCD2 */
  def process(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {
    // 1. Отбор активных строк из существующей таблицы
    val existingActiveDF = Filters.filterByPartitionValue(
        existingDF,
        config.partitionConfig.colName,
        Active.value).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 2. Формирование дата фрейма строк, где есть изменения
    val changesDF = ScdFunctions.detectChanges(
        existingActiveDF,
        incomingDF,
        config.primaryKeyColumns,
        config.sensitiveKeysColumns).
      persist(StorageLevel.MEMORY_AND_DISK)

    val existingActiveSchema: StructType = existingDF.schema

    // 3. Формивание датафрейма с активными записями для которых нет изменений
    val unchangedActiveRecordsDF = ScdFunctions.castColumnsToTargetSchema(
      existingActiveSchema,
      ScdFunctions.getUnchangedActiveRecords(
        existingActiveDF,
        changesDF,
        config.primaryKeyColumns))

    // 4. Формирование датафрейма с данными, которые переходят в статус "неактульные"
    val updateExistingDF = ScdFunctions.castColumnsToTargetSchema(
      existingActiveSchema,
      ScdFunctions.expireOldRecords(
        existingActiveDF,
        changesDF,
        config.primaryKeyColumns,
        config.effectiveDateTo,
        config.partitionConfig.colName,
        config.technicalColumn.sysDateColumnName,
        config.technicalColumn.sysDateDttmColumnName)
    )

    // 5. Формирование датафрейма с обновленными и новыми данными
    val upsertRecordsDF = ScdFunctions.castColumnsToTargetSchema(
      existingActiveSchema, ScdFunctions.prepareUpsertRecords(
        changesDF,
        config.effectiveDateFrom,
        config.effectiveDateFrom,
        config.partitionConfig.colName,
        config.technicalColumn.sysDateColumnName,
        config.technicalColumn.sysDateDttmColumnName))

    // 6. Объединение все данных
    val scd2DF = ScdFunctions.combineDataFrames(unchangedActiveRecordsDF, updateExistingDF, upsertRecordsDF)

    // 7. Возврат результирующего датафрейма
    scd2DF
  }


}

object SCD2Processor extends App {

  val spark = SparkSession.builder()
    .appName("Test DataFrames")
    .master("local[*]")
    .getOrCreate()


  spark.sparkContext.setLogLevel("WARN")

  private val historicalDF = spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .csv("src/main/resources/scd2/config/historical_data.csv")

  private val incrementalDF = spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .csv("src/main/resources/scd2/config/incremental_data.csv")

  private val scd2Config = SCD4Config(
    primaryKeyColumns = Seq("user_id"),
    sensitiveKeysColumns = Seq("email", "address"),
    effectiveDateFrom = "effective_from_dttm",
    effectiveDateTo = "effective_to_dttm",
    technicalColumn = CurrentTechnicalColumns()
  )

  private val processor = new SCD4Processor(scd2Config)

  private val scd2DF = processor.process(historicalDF, incrementalDF)

  scd2DF.show(100)
  //  private val scd2WriteConfig = SCD2WriteConfig(
  //    scd2DF,
  //    "test",
  //    SCD2PartitioningConfig("active_flg", Seq(0, 1))
  //  )
  //
  //  SCD2Writer.write(scd2WriteConfig, spark)

  //Очистка кэша, если использовался во время активной сессии spark
  CacheManager.clearCache(spark) match {
    case Success(_) => println("Cache was cleared successfully.")
    case Failure(e) => println(s"Cache was cleared unsuccessfully: ${e.getMessage}")
  }
}
