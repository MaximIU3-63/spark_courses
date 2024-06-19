package spark.scd2

import org.apache.spark.sql.functions.{col, concat_ws, current_date, sha1, to_date}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import spark.utils.ContextBuilder

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.util.Date

object Defaults {
  val OpenRowDate: String = "9999-12-31"
  val CurrentDate: String = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val CurrentDttm: String = new Timestamp(new Date().getTime).toString
}

object SCD2 extends App with ContextBuilder {
  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  import Defaults._
  import spark.implicits._

  case class Action(
                     id: Int,
                     attr: String,
                     nm_1: String,
                     nm_2: String,
                     start_date: String,
                     end_date: String
                   )

  //Список полей витрины, по которым формируется значение функции sha1()
  val dstColsToSha: Seq[Column] = Seq(
    col("id"),
    col("attr"),
    col("nm_1"),
    col("nm_2")
  )

  //Список полей витрины для конкатенации
  val dstColsToConcat: Seq[Column] = Seq(
    col("attr"),
    col("nm_1"),
    col("nm_2"),
    col("start_date")
  )

  //Список полей инкремента, по которым формируется значение функции sha1()
  val srcColsToConcat: Seq[Column] = Seq(
    col("src_attr"),
    col("nm_1"),
    col("nm_2")
  )

  //Список полей инкремента для конкатенации
  val srcColsToSha: Seq[Column] = Seq(
    col("src_id"),
    col("src_attr"),
    col("nm_1"),
    col("nm_2")
  )

  def writeAsCsv(fileName: String, df: DataFrame) = {
    df.
      repartition(1).
      write.
      mode("overwrite").
      option("header", "true").
      csv(s"src/main/resources/data/ntf/$fileName")
  }

  //Функция извлечения необходимых полей из датафрейма
  def extractCols(cols: Seq[Column])(df: DataFrame): DataFrame = {
    df.
      select(
        cols: _*
      )
  }

  //Фильтрация данных. Отбор открытых строк, где end_date = 9999-12-31
  def isOpenRow(df: DataFrame): DataFrame = {
    df.filter(
      to_date(col("end_date")) === OpenRowDate
    )
  }

  //Фильтрация данных. Отбор закрытых строк, где end_date < текущей даты
  def isClosedRow(df: DataFrame): DataFrame = {
    df.filter(
      to_date(col("end_date")) < current_date()
    )
  }

  //Функция по созданию колонки в датафрейма по входному условию
  def withColumn(colName: String, condition: Column)(df: DataFrame): DataFrame = {
    df.withColumn(colName, condition)
  }

  //Функция по созданию колонки с хэш суммой в датафрейме по определенным полям
  def withSha1Column(colName: String, colsToSha: Seq[Column])(df: DataFrame): DataFrame = {
    val condition = sha1(concat_ws("salt", colsToSha: _*))
    df.
      transform(withColumn(colName, condition))
  }

  //Join типа FULL между витриной и инкрементом по ключевому полю id -> src_id
  def joinFullDF(srcDF: DataFrame)(dstDF: DataFrame): DataFrame = {
    val joinCondition = col("id") === col("src_id")
    dstDF.
      join(srcDF, joinCondition, "full")
  }

  //Фильтрация датафрейма по условию, задаваемому параметром condition
  def filterByCondition(condition: Column)(df: DataFrame): DataFrame = {
    df.filter(condition)
  }

  //Извлечение данных, которые не требуется изменять.
  def extractNoActionRows(df: DataFrame): DataFrame = {
    df.
      map(row => {
        val unzippedConcatCol = row.getString(1).split(",")
        Action(
          row.getInt(0),
          unzippedConcatCol(0),
          unzippedConcatCol(1),
          unzippedConcatCol(2),
          unzippedConcatCol(3),
          row.getString(3)
        )
      }).toDF()
  }

  //Извлечение данных, которые необходимо закрыть
  def extractUpsertRows(df: DataFrame): DataFrame = {
    df.
      map(row => {
        val unzippedConcatCol = row.getString(1).split(",")
        Action(
          row.getInt(0),
          unzippedConcatCol(0),
          unzippedConcatCol(1),
          unzippedConcatCol(2),
          unzippedConcatCol(3),
          CurrentDate
        )
      }).toDF()
  }

  //Извлечение новыз данных из инкремента
  def extractNewRows(df: DataFrame): DataFrame = {
    df.
      map(row => {
        val unzippedConcatCol = row.getString(5).split(",")
        Action(
          row.getInt(4),
          unzippedConcatCol(0),
          unzippedConcatCol(1),
          unzippedConcatCol(2),
          CurrentDate,
          OpenRowDate
        )
      }).toDF()
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

  //Аналог функции show() с возможность указания сообщения на вывод.
  def display(df: DataFrame, truncate: Boolean = false)(message: String = ""): Unit = {
    if(message.nonEmpty) {
      println(message)
    }
    df.show(truncate)
  }

  val targetList = List(
    Row(1, "Hello!", "qwe", "qwe", "2020-01-01", "2021-01-31"),
    Row(1, "Hadoop", "qe", "qe", "2019-01-01", "9999-12-31"),
    Row(2, "Hadoop with Java", "e", "e", "2019-02-01", "9999-12-31"),
    Row(3, "old system", "we", "we", "2019-02-01", "9999-12-31"),
    Row(4, "Scala", "qe", "qe", "2019-02-02", "9999-12-31"),
    Row(6, "C", "ad", "ad", "2019-02-04", "9999-12-31")
  )

  val sourcelist = List(
    Row(1, "Spark", "qwe", "qwe"),
    Row(2, "PySpark!", "e", "e"),
    Row(4, "Scala", "qe", "qe"),
    Row(5, "C++!", "er", "er")
  )

  val targetSchema = StructType(
    List(
      StructField("id", IntegerType, true),
      StructField("attr", StringType, true),
      StructField("nm_1", StringType, true),
      StructField("nm_2", StringType, true),
      StructField("start_date", StringType, true),
      StructField("end_date", StringType, true)))

  val targetDF = spark.
    createDataFrame(spark.sparkContext.parallelize(targetList), targetSchema)

  /*
    Извлечение данных из витрины
    В фукнцию concat передаются колонки, которые в дальнейшем не участвуют в обработки.
    Необходимо для уменьшения размера таблицы перед джоином
     */
  val targetFilteredCols = Seq(
    col("id"),
    concat_ws(",", dstColsToConcat: _*).as("dst_concat_cols"),
    col("dst_sha"),
    col("end_date")
  )

  val targetFilteredDF = targetDF.
    transform(isOpenRow).
    transform(withSha1Column("dst_sha", dstColsToSha)).
    transform(extractCols(targetFilteredCols))

  //display(targetFilteredDF)("Target DataFrame:")

  val sourceSchema = StructType(List(
    StructField("src_id", IntegerType, true),
    StructField("src_attr", StringType, true),
    StructField("nm_1", StringType, true),
    StructField("nm_2", StringType, true)
  ))

  /*
  Извлечение данных из инкремента
  В фукнцию concat передаются колонки, которые в дальнейшем не участвуют в обработки.
  Необходимо для уменьшения размера таблицы перед джоином
   */
  val srcFilteredCols = Seq(
    col("src_id"),
    concat_ws(",", srcColsToConcat: _*).as("src_concat_cols"),
    col("src_sha")
  )

  //Формирование инкремента
  val sourceDF = spark.
    createDataFrame(spark.sparkContext.parallelize(sourcelist), sourceSchema).
    transform(withSha1Column("src_sha", srcColsToSha)).
    transform(extractCols(srcFilteredCols))


  //display(sourceDF)("Source DataFrame:")

  val joinDF = targetFilteredDF.
    transform(joinFullDF(sourceDF))

  //display(joinDF)("Join Result:")

  //Датафрейм с закрытыми строками из витрины
  val closedRowsDF = targetDF.
    transform(isClosedRow)

  writeAsCsv("closedRows", closedRowsDF)
  //display(closedRowsDF)("Closed rows:")

  //Формирование данных, которые не требуют изменения
  val noActionRowsCondition =
    col("src_sha").isNull ||
      col("src_sha") === col("dst_sha")

  val noActionDF = joinDF.
    transform(filterByCondition(noActionRowsCondition)).
    transform(extractNoActionRows)

  writeAsCsv("noActionRows", noActionDF)
  //display(noActionDF)("No action Dataframe:")

  //Формирование данные, которые необходимо закрыть в витрине на день расчета
  val upsertRowsCondition =
    col("src_id") === col("id") &&
      col("src_sha") =!= col("dst_sha")

  val upsertDF = joinDF.
    transform(filterByCondition(upsertRowsCondition)).
    transform(extractUpsertRows)

  writeAsCsv("upsertRows", upsertDF)
  //display(upsertDF)("Upsert Dataframe:")

  //Формирование новых данных из инкремента
  val newRowsCondition = {
    col("dst_sha").isNull ||
      (
        col("src_id") === col("id") &&
          col("src_sha") =!= col("dst_sha")
        )
  }

  val newRowsDF = joinDF.
    transform(filterByCondition(newRowsCondition)).
    transform(extractNewRows)

  writeAsCsv("newRows", newRowsDF)
  //display(newRowsDF)("Dataframe with new rows:")

  //Формирование обновленной витрины с данными
  val scd2DF = closedRowsDF.
    transform(
      unionDFs(
        noActionDF,
        upsertDF,
        newRowsDF
      )
    )

  writeAsCsv("scd2Rows", scd2DF)
  //display(scd2DF)("Result scd2 Dataframe:")

  spark.close()
}