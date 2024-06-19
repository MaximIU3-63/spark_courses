package spark.scd2

import org.apache.spark.sql.functions.{col, concat_ws, current_date, sha1, to_date}
import org.apache.spark.sql.{Column, DataFrame, Row}
import spark.utils.ContextBuilder

import scala.language.implicitConversions

object SCD2_test extends App with ContextBuilder {
  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  import Defaults._
  import spark.implicits._

  case class DmClient(
                       mdm_id: String,
                       customer_global_rk: String,
                       subscription_from_dt: String,
                       subscription_to_dt: String,
                       notification_package_nm: String,
                       notification_enable_flg: String,
                       notification_enable_dt: String,
                       notification_prolongation_dt: String,
                       push_flg: Int,
                       segment_revenue_nm: String,
                       segment_bank_nm: String,
                       retiree_flg: Int,
                       salary_flg: Int,
                       strategic_salary_flg: String,
                       segment_notification_nm: String,
                       base_payer_flg: Int,
                       effective_from_dt: String,
                       effective_to_dt: String,
                       sysdate_dttm: String,
                       sysdate_dt: String
                     )

  //Список полей витрины, по которым формируется значение функции sha1()
  private val dmColsToSha: Seq[Column] = Seq(
    col("mdm_id"),
    col("customer_global_rk"),
    col("subscription_from_dt"),
    col("subscription_to_dt"),
    col("notification_package_nm"),
    col("notification_enable_flg"),
    col("notification_enable_dt"),
    col("notification_prolongation_dt"),
    col("push_flg"),
    col("segment_revenue_nm"),
    col("segment_bank_nm"),
    col("retiree_flg"),
    col("salary_flg"),
    col("strategic_salary_flg"),
    col("segment_notification_nm"),
    col("base_payer_flg")
  )

  //Список полей витрины для конкатенации
  private val dmColsToConcat: Seq[Column] = Seq(
    col("customer_global_rk"),
    col("subscription_from_dt"),
    col("subscription_to_dt"),
    col("notification_package_nm"),
    col("notification_enable_flg"),
    col("notification_enable_dt"),
    col("notification_prolongation_dt"),
    col("push_flg"),
    col("segment_revenue_nm"),
    col("segment_bank_nm"),
    col("retiree_flg"),
    col("salary_flg"),
    col("strategic_salary_flg"),
    col("segment_notification_nm"),
    col("base_payer_flg"),
    col("effective_from_dt"),
    col("sysdate_dttm"),
    col("sysdate_dt")
  )

  //Список полей инкремента, по которым формируется значение функции sha1()
  private val incrementColsToConcat: Seq[Column] = dmColsToConcat

  //Список полей инкремента для конкатенации
  private val incrementColsToSha: Seq[Column] = dmColsToSha

  //Фильтрация данных. Отбор открытых строк, где effective_to_dt = 9999-12-31
  def isOpenRow(df: DataFrame): DataFrame = {
    df.filter(
      to_date(col("effective_to_dt")) === OpenRowDate
    )
  }

  //Фильтрация данных. Отбор закрытых строк, где effective_to_dt < текущей даты
  def isClosedRow(df: DataFrame): DataFrame = {
    df.filter(
      to_date(col("effective_to_dt")) < current_date()
    )
  }

  //Функция по созданию колонки с хэш суммой в датафрейме по определенным полям
  def withSha1Column(colName: String, colsToSha: Seq[Column])(df: DataFrame): DataFrame = {
    val condition = sha1(concat_ws("salt", colsToSha: _*))
    df.
      transform(withColumn(colName, condition))
  }

  //Join типа FULL между витриной и инкрементом по ключевому полю mdm_id -> inc_mdm_id
  def joinFullDF(incrementDF: DataFrame)(dmDF: DataFrame): DataFrame = {
    val joinCondition = col("mdm_id") === col("inc_mdm_id")
    dmDF.
      join(incrementDF, joinCondition, "full")
  }

  //Функция извлечения необходимых полей из датафрейма
  def extractCols(cols: Seq[Column])(df: DataFrame): DataFrame = {
    df.
      select(
        cols: _*
      )
  }

  //Функция по созданию колонки в датафрейма по входному условию
  def withColumn(colName: String, condition: Column)(df: DataFrame): DataFrame = {
    df.withColumn(colName, condition)
  }

  //Фильтрация датафрейма по условию, задаваемому параметром condition
  def filterDataByCondition(condition: Column)(df: DataFrame): DataFrame = {
    df.filter(condition)
  }

  //Объединение датафреймов с разным типом действия
  def unionDFs(noActionDF: DataFrame,
               upsertDF: DataFrame,
               newRowsDF: DataFrame)(closedDF: DataFrame): DataFrame = {
    closedDF.
      union(noActionDF).
      union(upsertDF).
      union(newRowsDF)
  }

  def getNoActionRows(df: DataFrame): DataFrame = {
    df.map(row => {
      val unzippedConcatCol = row.getString(1).split(",")
      DmClient(
        row.getString(0),
        unzippedConcatCol(0),
        unzippedConcatCol(1),
        unzippedConcatCol(2),
        unzippedConcatCol(3),
        unzippedConcatCol(4),
        unzippedConcatCol(5),
        unzippedConcatCol(6),
        unzippedConcatCol(7).toInt,
        unzippedConcatCol(8),
        unzippedConcatCol(9),
        unzippedConcatCol(10).toInt,
        unzippedConcatCol(11).toInt,
        unzippedConcatCol(12),
        unzippedConcatCol(13),
        unzippedConcatCol(14).toInt,
        unzippedConcatCol(15),
        row.getString(3),
        CurrentDate,
        CurrentDttm
      )
    }).toDF()
  }

  def getUpsertActionRows(df: DataFrame): DataFrame = {
    df.map(row => {
      val unzippedConcatCol = row.getString(1).split(",")
      DmClient(
        row.getString(0),
        unzippedConcatCol(0),
        unzippedConcatCol(1),
        unzippedConcatCol(2),
        unzippedConcatCol(3),
        unzippedConcatCol(4),
        unzippedConcatCol(5),
        unzippedConcatCol(6),
        unzippedConcatCol(7).toInt,
        unzippedConcatCol(8),
        unzippedConcatCol(9),
        unzippedConcatCol(10).toInt,
        unzippedConcatCol(11).toInt,
        unzippedConcatCol(12),
        unzippedConcatCol(13),
        unzippedConcatCol(14).toInt,
        unzippedConcatCol(15),
        CurrentDate,
        CurrentDate,
        CurrentDttm
      )
    }).toDF()
  }

  def getNewActionRows(df: DataFrame): DataFrame = {
    df.map(row => {
      val unzippedConcatCol = row.getString(5).split(",")
      DmClient(
        row.getString(4),
        unzippedConcatCol(0),
        unzippedConcatCol(1),
        unzippedConcatCol(2),
        unzippedConcatCol(3),
        unzippedConcatCol(4),
        unzippedConcatCol(5),
        unzippedConcatCol(6),
        unzippedConcatCol(7).toInt,
        unzippedConcatCol(8),
        unzippedConcatCol(9),
        unzippedConcatCol(10).toInt,
        unzippedConcatCol(11).toInt,
        unzippedConcatCol(12),
        unzippedConcatCol(13),
        unzippedConcatCol(14).toInt,
        CurrentDate,
        OpenRowDate,
        CurrentDate,
        CurrentDttm //
      )
    }).toDF()
  }


  val dataMartDF = Seq(
    ("1", "11", "2024-03-01", "2024-04-01", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-21", "2024-04-23", "2024-04-25 12:00:00", "2024-03-25"),
    ("2", "31", "2024-03-03", "2024-04-03", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-21", "2024-04-23", "2024-04-25 12:00:00", "2024-03-25"),
    ("3", "41", "2023-03-01", "2023-04-01", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-25", "9999-12-31", "2024-04-25 12:00:00", "2024-03-25"),
    ("4", "51", "2024-01-01", "2024-02-01", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-25", "9999-12-31", "2024-04-25 12:00:00", "2024-03-25"),
    ("5", "61", "2024-03-01", "2024-04-01", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-25", "9999-12-31", "2024-04-25 12:00:00", "2024-03-25"),
    ("6", "71", "2024-02-04", "2024-03-04", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-25", "9999-12-31", "2024-04-25 12:00:00", "2024-03-25"),
    ("7", "81", "2024-03-12", "2024-04-12", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-25", "9999-12-31", "2024-04-25 12:00:00", "2024-03-25")
  ).toDF(
    "mdm_id",
    "customer_global_rk",
    "subscription_from_dt",
    "subscription_to_dt",
    "notification_package_nm",
    "notification_enable_flg",
    "notification_enable_dt",
    "notification_prolongation_dt",
    "push_flg",
    "segment_revenue_nm",
    "segment_bank_nm",
    "retiree_flg",
    "salary_flg",
    "strategic_salary_flg",
    "segment_notification_nm",
    "base_payer_flg",
    "effective_from_dt",
    "effective_to_dt",
    "sysdate_dttm",
    "sysdate_dt")

  val incrementDF = Seq(
    ("1", "11", "2024-03-01", "2024-04-01", "vip", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 0, "rt", 1, "2024-04-26", "9999-12-31", "2024-04-26 12:00:00", "2024-04-26"),
    ("2", "31", "2024-03-03", "2024-04-03", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-26", "9999-12-31", "2024-04-26 12:00:00", "2024-04-26"),
    ("3", "41", "2023-03-01", "2023-04-01", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-26", "9999-12-31", "2024-04-26 12:00:00", "2024-04-26"),
    ("4", "51", "2024-01-01", "2024-02-01", "massoviy", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-26", "9999-12-31", "2024-04-26 12:00:00", "2024-04-26"),
    ("6", "71", "2024-02-04", "2024-03-04", "vip", 1, "2024-03-01", "2024-04-01", 2, "q", "w", 1, 1, 1, "rt", 1, "2024-04-25", "9999-12-31", "2024-04-25 12:00:00", "2024-03-25"),
    ("12","61", "2024-03-01", "2024-04-01", "tolk", 1, "2024-03-01", "2024-04-01", 1, "q", "w", 1, 1, 1, "rt", 1, "2024-04-26", "9999-12-31", "2024-04-26 12:00:00", "2024-04-26")
  ).toDF(
    "mdm_id",
    "customer_global_rk",
    "subscription_from_dt",
    "subscription_to_dt",
    "notification_package_nm",
    "notification_enable_flg",
    "notification_enable_dt",
    "notification_prolongation_dt",
    "push_flg",
    "segment_revenue_nm",
    "segment_bank_nm",
    "retiree_flg",
    "salary_flg",
    "strategic_salary_flg",
    "segment_notification_nm",
    "base_payer_flg",
    "effective_from_dt",
    "effective_to_dt",
    "sysdate_dttm",
    "sysdate_dt")

  /*
    Извлечение данных из витрины
    В фукнцию concat передаются колонки, которые в дальнейшем не участвуют в обработки.
    Необходимо для уменьшения размера таблицы перед джоином
     */
  val dataMartFilteredCols = Seq(
    col("mdm_id"),
    concat_ws(",", dmColsToConcat: _*).as("dm_concat_cols"),
    col("dm_sha"),
    col("effective_to_dt")
  )

  val dataMartFilteredDF = dataMartDF.
    transform(isOpenRow).
    transform(withSha1Column("dm_sha", dmColsToSha)).
    transform(extractCols(dataMartFilteredCols))

  /*
  Извлечение данных из инкремента
  В фукнцию concat передаются колонки, которые в дальнейшем не участвуют в обработки.
  Необходимо для уменьшения размера таблицы перед джоином
   */
  val incrementFilteredCols = Seq(
    col("mdm_id").as("inc_mdm_id"),
    concat_ws(",", incrementColsToConcat: _*).as("increment_concat_cols"),
    col("increment_sha")
  )

  //Формирование инкремента
  val sourceDF = incrementDF.
    transform(withSha1Column("increment_sha", incrementColsToSha)).
    transform(extractCols(incrementFilteredCols))

  val joinDF = dataMartFilteredDF.
    transform(joinFullDF(sourceDF))

  //Датафрейм с закрытыми строками из витрины
  val closedRowsDF = dataMartDF.
    transform(isClosedRow)

  println("closedRowsDF:")
  closedRowsDF.show(false)

  //Формирование данных, которые не требуют изменения
  val noActionRowsCondition =
    col("increment_sha").isNull ||
      col("increment_sha") === col("dm_sha")

  val noActionDF = joinDF.
    transform(filterDataByCondition(noActionRowsCondition)).
    transform(getNoActionRows)

  println("noActionDF:")
  noActionDF.show(false)

  //Формирование данные, которые необходимо закрыть в витрине на день расчета
  val upsertRowsCondition =
    col("inc_mdm_id") === col("mdm_id") &&
      col("increment_sha") =!= col("dm_sha")

  val upsertDF = joinDF.
    transform(filterDataByCondition(upsertRowsCondition)).
    transform(getUpsertActionRows)

  println("upsertDF:")
  upsertDF.show(false)

  //Формирование новых данных из инкремента
  val newRowsCondition = {
    col("dm_sha").isNull ||
      (
        col("inc_mdm_id") === col("mdm_id") &&
          col("increment_sha") =!= col("dm_sha")
        )
  }

  val newRowsDF = joinDF.
    transform(filterDataByCondition(newRowsCondition)).
    transform(getNewActionRows)

  println("newRowsDF:")
  newRowsDF.show(false)

//  //Формирование обновленной витрины с данными
//  val scd2DF = closedRowsDF.
//    transform(
//      unionDFs(
//        noActionDF,
//        upsertDF,
//        newRowsDF
//      )
//    )
//
//  scd2DF.show(false)

  spark.close()
}