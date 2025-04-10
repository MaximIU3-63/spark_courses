package spark.scd2

import org.apache.spark.sql.functions.{col, concat, current_date, current_timestamp, lit, sha1}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.scd2.utils.{Active, CacheManager, Constants, DateFormat, Dates, Filters, ISO_8601, ISO_8601_EXTENDED, Inactive, Messages, SCD2Defaults, SCD2PartitioningConfig}

import scala.util.{Failure, Success}

//Трейт с описанием базовых технических полей
trait BaseTechnicalColumns {
  val sysDateColumnName: String
  val sysDateDttmColumnName: String
}

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
case class SCD2Config(
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

class SCD2Processor(config: SCD2Config) {

  /** Основной метод обработки SCD2 */
  def process(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {
    // 1. Отбор активных строк из существующей таблицы
    val existingActiveDF = Filters.filterByPartitionValue(
        existingDF,
        config.partitionConfig.colName,
        Active.value).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 2. Формирование дата фрейма строк, где есть изменения
    val changesDF = detectChanges(existingActiveDF, incomingDF).
      persist(StorageLevel.MEMORY_AND_DISK)

    val existingActiveSchema: StructType = existingDF.schema

    // 3. Формивание датафрейма с активными записями для которых нет изменений
    val unchangedActiveRecordsDF = castColumnsToTargetSchema(existingActiveSchema, getUnchangedActiveRecords(existingActiveDF, changesDF))

    // 4. Формирование датафрейма с данными, которые переходят в статус "неактульные"
    val updateExistingDF = castColumnsToTargetSchema(existingActiveSchema, expireOldRecords(existingActiveDF, changesDF))

    // 5. Формирование датафрейма с обновленными и новыми данными
    val upsertRecordsDF = castColumnsToTargetSchema(existingActiveSchema, prepareUpsertRecords(changesDF))

    // 6. Объединение все данных
    val scd2DF = combineDataFrames(unchangedActiveRecordsDF, updateExistingDF, upsertRecordsDF)

    // 7. Возврат результирующего датафрейма
    scd2DF
  }

  /** Детектирование изменений с использованием хеширования */
  private def detectChanges(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    val hashColumns = (config.primaryKeyColumns ++ config.sensitiveKeysColumns).map(col)

    val existingWithHashDF = existingDF.
      select(config.primaryKeyColumns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.HASH_COLUMN): _*)

    val incomingWithHashDF = incomingDF.
      select(incomingDF.columns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.HASH_COLUMN): _*)

    incomingWithHashDF.alias("incoming").
      join(existingWithHashDF.alias("existing"), Seq(Constants.HASH_COLUMN), "left_anti").
      select(incomingDF.columns.map(col): _*)
  }

  /** Нахождение активных записей без изменений */
  private def getUnchangedActiveRecords(existingActiveDF: DataFrame, changesDF: DataFrame): DataFrame = {
    existingActiveDF.join(
      changesDF.select(config.primaryKeyColumns.map(col): _*),
      config.primaryKeyColumns,
      "left_anti"
    )
  }

  /** Закрытие устаревших записей */
  private def expireOldRecords(existingActiveDF: DataFrame, changesDF: DataFrame): DataFrame = {
    existingActiveDF.
      join(changesDF, config.primaryKeyColumns, "left_semi").
      withColumn(config.effectiveDateTo, DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)).
      withColumn(config.partitionConfig.colName, lit(Inactive.value)).
      withColumn(config.technicalColumn.sysDateColumnName, lit(DateFormat.formatDateColumn(current_date(), ISO_8601))).
      withColumn(config.technicalColumn.sysDateDttmColumnName, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
  }

  /** Формирование дата фрейма с новыми и обновленными данными */
  private def prepareUpsertRecords(changesDF: DataFrame): DataFrame = {
    changesDF
      .withColumn(config.effectiveDateFrom, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
      .withColumn(config.effectiveDateTo, lit(Dates.closeDateDttmValue))
      .withColumn(config.partitionConfig.colName, lit(Active.value))
      .withColumn(config.technicalColumn.sysDateColumnName, lit(DateFormat.formatDateColumn(current_date(), ISO_8601)))
      .withColumn(config.technicalColumn.sysDateDttmColumnName, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
  }

  /** Объединение дата фреймов */
  private def combineDataFrames(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_ unionByName _)
      .orderBy(config.primaryKeyColumns.map(col): _*)

  /** Приведение полей дата фрейма к целевому маппингу и типу данных */
  private def castColumnsToTargetSchema(prioritySchema: StructType, df: DataFrame): DataFrame = {
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

  private val scd2Config = SCD2Config(
    primaryKeyColumns = Seq("user_id"),
    sensitiveKeysColumns = Seq("email", "address"),
    effectiveDateFrom = "effective_from_dttm",
    effectiveDateTo = "effective_to_dttm",
    technicalColumn = CurrentTechnicalColumns()
  )

  private val processor = new SCD2Processor(scd2Config)

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
