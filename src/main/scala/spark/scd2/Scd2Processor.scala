package spark.scd2

import org.apache.spark.sql.functions.{coalesce, col, concat, current_timestamp, lit, sha1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.scd2.utils.{CacheManager, Constants, DateFormat, Dates, Filters, ISO_8601_EXTENDED, Messages}

import java.sql.Timestamp
import scala.util.{Failure, Success}

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
    existingDF.filter(Filters.isNonActualRecord(config.isActiveCol))
  }

  // Извлечение исторических актуальных записей
  private def getActiveRecords(existingDF: DataFrame): DataFrame = {
    existingDF.filter(Filters.isActualRecord(config.isActiveCol))
  }

  // Формирование датафрейма с изменеными или новыми записями
  private def detectChanges(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    val hashColumns = (config.primaryKeyColumns ++ config.sensitiveKeysColumns).map(col)

    val existingWithHashDF = existingDF.
      select(hashColumns :+ sha1(concat(hashColumns: _*)).as(Constants.SHA): _*)

    val incomingWithHashDF = incomingDF.
      select(hashColumns :+ sha1(concat(hashColumns: _*)).as(Constants.SHA): _*)

    existingWithHashDF.alias("existing").
      join(incomingWithHashDF.as("incoming"), config.primaryKeyColumns, "full_outer").
      filter(coalesce(
        existingWithHashDF(Constants.SHA) =!= incomingWithHashDF(Constants.SHA),
        existingWithHashDF(Constants.SHA).isNotNull || incomingWithHashDF(Constants.SHA).isNotNull
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
      drop(Constants.SHA).
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

  val existingDF = Seq(
    (1, "base", "john@example.com", "Street 1", Timestamp.valueOf("2023-01-01 00:00:00"), Timestamp.valueOf("5999-12-31 00:00:00"), true),
    (2, "base","may@example.com", "Street 2", Timestamp.valueOf("2023-03-08 00:00:00"), Timestamp.valueOf("2023-12-31 00:00:00"), false),
    (3, "vip","sasha@example.com", "Street 3", Timestamp.valueOf("2023-05-11 00:00:00"), Timestamp.valueOf("5999-12-31 00:00:00"), true)
  ).toDF("customer_id", "client_type", "email", "address", "effective_from_dt", "effective_to_dt", "is_active")

  val updatesDF = Seq(
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

  val scd2DF = processor.process(existingDF, updatesDF)

  scd2DF.show(false)

  //Очистка кэша, если использовался в время активной сессии spark
  CacheManager.clearCache(spark) match {
    case Success(_) => println("Cache was cleared successfully.")
    case Failure(e) => println(s"Cache was cleared unsuccessfully: ${e.getMessage}")
  }
}