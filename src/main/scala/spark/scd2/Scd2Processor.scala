package spark.scd2

import org.apache.spark.sql.functions.{coalesce, col, concat, current_timestamp, date_format, lit, sha1}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

private object Dates {
  lazy val closeDateValue: String = "5999-12-31 00:00:00"
}

private trait DateFormatter

private case object ISO_8601 extends DateFormatter {
  override implicit def toString: String = "yyyy-MM-dd"
}

private case object ISO_8601_EXTENDED extends DateFormatter {
  override implicit def toString: String = "yyyy-MM-dd HH:mm:ss.SSS"
}

private object DateFormat {
  def formatDateColumn(column: Column, format: DateFormatter): Column = {
    date_format(column, format.toString)
  }
}

// Объект с описаниями правил отбора актуальных и неактуальных записей
private object Filter {
  lazy val isActualRecord: String => Column = (colName: String) => col(colName) === lit(true)
  lazy val isNonActualRecord: String => Column = (colName: String) => col(colName) === lit(false)
}

private object Const {
  val SHA: String = "sha1"
}

private object Messages {

  lazy val requireMessage: String => String = (value: String) => {
    s"""
      | Val $value shouldn't be empty.
      |""".stripMargin
  }
}

private object MemoryManager {
  // Функция очистики кэша
  def clearCache(spark: SparkSession): Try[Unit] = Try {
    spark.sharedState.cacheManager.clearCache()
  }
}

private case class Scd2Config(
                       primaryKeyColumns: Seq[String],
                       sensitiveKeysColumns: Seq[String],
                       effectiveDateFrom: String = "effective_from_dt",
                       effectiveDateTo: String = "effective_to_dt",
                       isActiveCol: String = "is_active"
                     ) {
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("primaryKeyColumns"))
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("sensitiveKeysColumns"))
}

private class Scd2Processor(config: Scd2Config) {
  def process(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    // 1. Отбор строк, которые не трубуется проверять
    val existingNoActiveDF = getOldRecords(existingDF)

    // 2. Отбор активных строк из существующей таблицы
    val existingActiveDF = getActiveRecords(existingDF).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 3. Формирование датафрейма строк, где есть изменения
    val changesDF = detectChanges(existingActiveDF, incomingDF).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 4. Формирование датафрейма с данными, которые переходят в статус "неактульные"
    val updateExistingDF = expireOldRecords(existingActiveDF, changesDF)

    // 5. Формирование датафрейма с обновленными и новыми данными
    val upsertRecordsDF = prepareUpsertRecords(changesDF)

    // 6. Объединение все данных
    val scd2DF = unionAll(existingNoActiveDF, updateExistingDF, upsertRecordsDF)

    scd2DF
  }

  // Извлечение исторических неактульных записей
  private def getOldRecords(existingDF: DataFrame): DataFrame = {
    existingDF.filter(Filter.isNonActualRecord(config.isActiveCol))
  }

  // Извлечение исторических актуальных записей
  private def getActiveRecords(existingDF: DataFrame): DataFrame = {
    existingDF.filter(Filter.isActualRecord(config.isActiveCol))
  }

  // Формирование датафрейма с изменеными или новыми записями
  private def detectChanges(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    val hashColumns = (config.primaryKeyColumns ++ config.sensitiveKeysColumns).map(col)

    val existingWithHashDF = existingDF.
      select(hashColumns :+ sha1(concat(hashColumns: _*)).as(Const.SHA): _*)

    val incomingWithHashDF = incomingDF.
      select(hashColumns :+ sha1(concat(hashColumns: _*)).as(Const.SHA): _*)

    existingWithHashDF.alias("existing").
      join(incomingWithHashDF.as("incoming"), config.primaryKeyColumns, "full_outer").
      filter(coalesce(
        existingWithHashDF(Const.SHA) =!= incomingWithHashDF(Const.SHA),
        existingWithHashDF(Const.SHA).isNotNull || incomingWithHashDF(Const.SHA).isNotNull
      )).
      selectExpr("incoming.*")
  }

  /*
    Формирование датафрейма с историческими актуальными данными, по котором пришло обновление
    и которые необходимо закрыть
   */
  private def expireOldRecords(existingActiveDF: DataFrame, changesDF: DataFrame): DataFrame = {
    val joinCondition = config.primaryKeyColumns.
      map(colName => existingActiveDF(colName) === changesDF(colName)).
      reduce(_ && _)

    existingActiveDF.join(changesDF, joinCondition, "left_semi").
      withColumn(config.effectiveDateTo, DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)).
      withColumn(config.isActiveCol, lit(false))
  }

  // Формирование датафрейма с новыми и обновленными данными
  private def prepareUpsertRecords(changesDF: DataFrame): DataFrame = {
    changesDF.
      drop(Const.SHA).
      withColumn(config.effectiveDateFrom, DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)).
      withColumn(config.effectiveDateTo, lit(Dates.closeDateValue)).
      withColumn(config.isActiveCol, lit(true))
  }

  // Объединение датафреймов
  private def unionAll(dfs: DataFrame*): DataFrame = {
    dfs.reduce((df1, df2) => df1.unionByName(df2)).
      orderBy(config.primaryKeyColumns.map(col): _*)
  }
}

object Scd2Processor extends App {
  
  val spark = SparkSession.builder()
    .appName("Test DataFrames")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val existing = Seq(
    (1, "base", "john@example.com", "Street 1", Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("5999-12-31 00:00:00"), true),
    (2, "base","may@example.com", "Street 2", Timestamp.valueOf("2023-03-08 00:00:00"), Timestamp.valueOf("2023-12-31 00:00:00"), false),
    (3, "vip","sasha@example.com", "Street 3", Timestamp.valueOf("2023-05-11 00:00:00"), Timestamp.valueOf("5999-12-31 00:00:00"), true)
  ).toDF("customer_id", "client_type", "email", "address", "effective_from_dt", "effective_to_dt", "is_active")

  val updates = Seq(
    (1, "vip", "john_new@example.com", "Street 100"),
    (2, "base", "wk@mail.ru", "Walking street"),
    (4, "vip" ,"yana@gmail.com", "Agrba 100"),
    (3, "vip","sasha@example.com", "Street 3")
  ).toDF("customer_id", "client_type", "email", "address")

  private val processor = new Scd2Processor(
    Scd2Config(
      primaryKeyColumns = Seq("customer_id"),
      sensitiveKeysColumns = Seq("client_type", "email", "address")
    )
  )

  val scd2DF = processor.process(existing, updates)

  scd2DF.show(false)

  //Очистка кэша, если использовался в время активной сессии spark
  MemoryManager.clearCache(spark) match {
    case Success(_) => println("Cache was cleared successfully.")
    case Failure(e) => println(s"Cache was cleared unsuccessfully: ${e.getMessage}")
  }
}