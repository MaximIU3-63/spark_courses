package spark.scd2

import org.apache.spark.sql.functions.{coalesce, col, concat, current_date, current_timestamp, lit, sha1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.scd2.utils.{CacheManager, Constants, DateFormat, Dates, Filters, ISO_8601, ISO_8601_EXTENDED, Messages}

import scala.util.{Failure, Success}

case class TechnicalColumns(
                             sysdateDt: String = "sysdate_dt",
                             sysdateDttm: String = "sysdate_dttm"
                           ) {
  // Кэшируем список технических колонок
  val technicalColNameList: List[String] = List(sysdateDt, sysdateDttm)
}

private case class Scd2Config(
                               primaryKeyColumns: Seq[String],
                               sensitiveKeysColumns: Seq[String],
                               effectiveDateFrom: String = "effective_from_dt",
                               effectiveDateTo: String = "effective_to_dt",
                               isActiveCol: String = "is_active",
                               technicalColumn: TechnicalColumns
                             ) {
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("primaryKeyColumns"))
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("sensitiveKeysColumns"))
}

private class Scd2Processor(config: Scd2Config) {

  /** Основной метод обработки SCD2 */
  def process(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    // 1. Отбор строк, которые не трубуется обрабатывать
    val existingNoActiveDF = filterNonActive(existingDF)

    // 2. Отбор активных строк из существующей таблицы
    val existingActiveDF = filterActive(existingDF).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 3. Формирование датафрейма строк, где есть изменения
    val changesDF = detectChanges(existingActiveDF, incomingDF).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 4. Формирование датафрейма с данными, которые переходят в статус "неактульные"
    val updateExistingDF = expireOldRecords(existingActiveDF, changesDF)

    // 5. Формирование датафрейма с обновленными и новыми данными
    val upsertRecordsDF = prepareUpsertRecords(changesDF)

    // 6. Объединение все данных
    val scd2DF = combineDataFrames(existingNoActiveDF, updateExistingDF, upsertRecordsDF)

    // 7. Возврат результирующего датафрейма
    scd2DF
  }

  /** Фильтрация неактивных записей */
  private def filterNonActive(existingDF: DataFrame): DataFrame =
    existingDF.filter(Filters.isNonActualRecord(config.isActiveCol))

  /** Фильтрация активных записей */
  private def filterActive(existingDF: DataFrame): DataFrame =
    existingDF.filter(Filters.isActualRecord(config.isActiveCol))

  /** Детектирование изменений с использованием хеширования */
  private def detectChanges(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    val hashColumns = (config.primaryKeyColumns ++ config.sensitiveKeysColumns).map(col)

    val existingWithHashDF = existingDF.
      select(config.primaryKeyColumns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.SHA): _*)

    val incomingWithHashDF = incomingDF.
      select(incomingDF.columns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.SHA): _*)

    existingWithHashDF.alias("existing").
      join(incomingWithHashDF.as("incoming"), config.primaryKeyColumns, "full_outer").
      filter(coalesce(
        existingWithHashDF(Constants.SHA) =!= incomingWithHashDF(Constants.SHA),
        existingWithHashDF(Constants.SHA).isNotNull || incomingWithHashDF(Constants.SHA).isNotNull
      )).
      select(incomingDF.columns.map(col): _*)
  }

  /** Закрытие устаревших записей */
  private def expireOldRecords(existingActiveDF: DataFrame, changesDF: DataFrame): DataFrame = {

    val joinCondition = config.primaryKeyColumns.
      map(colName => existingActiveDF(colName) <=> changesDF(colName)).
      reduce(_ && _)

    existingActiveDF.join(changesDF, joinCondition, "left_semi").
      withColumn(config.effectiveDateTo, DateFormat.formatDateColumn(current_date(), ISO_8601)).
      withColumn(config.isActiveCol, lit("false"))
  }

  // Формирование датафрейма с новыми и обновленными данными
  private def prepareUpsertRecords(changesDF: DataFrame): DataFrame = {

    //    val commonColumns = {
    //      changesDF.
    //        columns.
    //        filter(col => !config.technicalColumn.technicalColNameList.contains(col) && col != Constants.SHA).
    //        map(col)
    //    }
    //
    //    changesDF.
    //      drop(Constants.SHA).
    //      select(
    //        commonColumns ++ Array(
    //          lit(DateFormat.formatDateColumn(current_date(), ISO_8601)).as(config.effectiveDateFrom),
    //          lit(Dates.closeDateValue).as(config.effectiveDateTo),
    //          lit("true").as(config.isActiveCol),
    //          lit(DateFormat.formatDateColumn(current_date(), ISO_8601)).as(config.technicalColumn.sysdateDt),
    //          lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)).as(config.technicalColumn.sysdateDttm)
    //        ): _*
    //      )
    changesDF
      .withColumn(config.effectiveDateFrom, lit(DateFormat.formatDateColumn(current_date(), ISO_8601)))
      .withColumn(config.effectiveDateTo, lit(Dates.closeDateValue))
      .withColumn(config.isActiveCol, lit("true"))
      .withColumn(config.technicalColumn.sysdateDt, lit(DateFormat.formatDateColumn(current_date(), ISO_8601)))
      .withColumn(config.technicalColumn.sysdateDttm, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
  }

  /** Объединение датафреймов */
  private def combineDataFrames(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_ unionByName _)
      .orderBy(config.primaryKeyColumns.map(col): _*)
}

object Scd2Processor extends App {

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

  private val scd2Config = Scd2Config(
    primaryKeyColumns = Seq("user_id"),
    sensitiveKeysColumns = Seq("email", "address"),
    technicalColumn = TechnicalColumns()
  )

  private val processor = new Scd2Processor(scd2Config)

  val scd2DF = processor.process(historicalDF, incrementalDF)

  scd2DF.show(truncate = false)

  //Очистка кэша, если использовался в время активной сессии spark
  CacheManager.clearCache(spark) match {
    case Success(_) => println("Cache was cleared successfully.")
    case Failure(e) => println(s"Cache was cleared unsuccessfully: ${e.getMessage}")
  }
}