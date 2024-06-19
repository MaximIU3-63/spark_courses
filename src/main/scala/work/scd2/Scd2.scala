//package work.scd2
//
//import org.apache.log4j.Logger
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import work.scd2.Scd2Functions._
//import work.JsonConfigs
//
//
//class Scd2(implicit spark: SparkSession, scd2Description: Scd2Description) {
//
//  private lazy val logger: Logger = Logger.getLogger(this.getClass)
//
//  spark.sparkContext.setLogLevel("WARN")
//  import spark.implicits._
//
//  private val joinCols = toSeqCol(jsonConfigs.join_cols)
//  private val incrementJoinCols = toSeqCol(
//    jsonConfigs.join_cols.map(col => s"src_$col")
//  )
//  private val concatCols = concat_ws(",", toSeqCol(jsonConfigs.insensitive_cols): _*)
//  private val shaCols = toSeqCol(jsonConfigs.sensitive_cols)
//  private val effectiveToCol = col(jsonConfigs.effective_to_col_name.get)
//
//  private def getNoActionRows(df: DataFrame): DataFrame = {
//    df.map(row => {
//      val unzippedConcatCol = row.getString(1).split(",")
//      DmClient(
//        row.getString(0),
//        unzippedConcatCol(0),
//        unzippedConcatCol(1),
//        unzippedConcatCol(2),
//        unzippedConcatCol(3),
//        unzippedConcatCol(4),
//        unzippedConcatCol(5),
//        unzippedConcatCol(6),
//        unzippedConcatCol(7).toInt,
//        unzippedConcatCol(8),
//        unzippedConcatCol(9),
//        unzippedConcatCol(10).toInt,
//        unzippedConcatCol(11).toInt,
//        unzippedConcatCol(12),
//        unzippedConcatCol(13),
//        unzippedConcatCol(14).toInt,
//        unzippedConcatCol(15),
//        row.getString(3),
//        "2024-06-13",
//        "2024-06-13"
//      )
//    }).toDF()
//  }
//
//  private def getUpsertActionRows(df: DataFrame): DataFrame = {
//    df.map(row => {
//      val unzippedConcatCol = row.getString(1).split(",")
//      DmClient(
//        row.getString(0),
//        unzippedConcatCol(0),
//        unzippedConcatCol(1),
//        unzippedConcatCol(2),
//        unzippedConcatCol(3),
//        unzippedConcatCol(4),
//        unzippedConcatCol(5),
//        unzippedConcatCol(6),
//        unzippedConcatCol(7).toInt,
//        unzippedConcatCol(8),
//        unzippedConcatCol(9),
//        unzippedConcatCol(10).toInt,
//        unzippedConcatCol(11).toInt,
//        unzippedConcatCol(12),
//        unzippedConcatCol(13),
//        unzippedConcatCol(14).toInt,
//        unzippedConcatCol(15),
//        "2024-06-13",
//        "2024-06-13",
//        "2024-06-13"
//      )
//    }).toDF()
//  }
//
//  private def getNewActionRows(df: DataFrame): DataFrame = {
//    df.map(row => {
//      val unzippedConcatCol = row.getString(5).split(",")
//      DmClient(
//        row.getString(4),
//        unzippedConcatCol(0),
//        unzippedConcatCol(1),
//        unzippedConcatCol(2),
//        unzippedConcatCol(3),
//        unzippedConcatCol(4),
//        unzippedConcatCol(5),
//        unzippedConcatCol(6),
//        unzippedConcatCol(7).toInt,
//        unzippedConcatCol(8),
//        unzippedConcatCol(9),
//        unzippedConcatCol(10).toInt,
//        unzippedConcatCol(11).toInt,
//        unzippedConcatCol(12),
//        unzippedConcatCol(13),
//        unzippedConcatCol(14).toInt,
//        "2024-06-13",
//        "2999-06-13",
//        "2024-06-13",
//        "2024-06-13"
//      )
//    }).toDF()
//  }
//
//  //Формирование scd2 представления данных
//  def formScd2Layer(inputDF: DataFrame, dataMartTable: String): DataFrame = {
//
//    val dataMartDF = spark.table(dataMartTable)
//
//    /*
//      Извлечение данных из витрины
//      В фукнцию concat передаются колонки, которые в дальнейшем не участвуют в обработки.
//      Необходимо для уменьшения размера таблицы перед джоином
//       */
//    val dataMartFilteredCols = joinCols.
//      appended(concatCols.as("dm_concat_cols")).
//      appended(col("dm_sha")).
//      appended(effectiveToCol)
//
//    val dataMartFilteredDF = dataMartDF.
//      transform(isOpenRow(effectiveToCol)).
//      transform(withSha1Column("dm_sha", shaCols)).
//      transform(extractCols(dataMartFilteredCols))
//
//    /*
//    Извлечение данных из инкремента
//    В фукнцию concat передаются колонки, которые в дальнейшем не участвуют в обработки.
//    Необходимо для уменьшения размера таблицы перед джоином
//     */
//    val incrementFilteredCols = incrementJoinCols.
//      appended(concatCols.as("increment_concat_cols")).
//      appended(col("increment_sha"))
//
//    //Формирование инкремента
//    val sourceDF = inputDF.
//      transform(withSha1Column("increment_sha", shaCols)).
//      transform(extractCols(incrementFilteredCols))
//
//    //Объединение данных витрины и инкремента
//    val joinDF = dataMartFilteredDF.
//      transform(joinFullDF(sourceDF, col("")))
//
//    //Датафрейм с закрытыми строками из витрины
//    val closedRowsDF = dataMartDF.
//      transform(isClosedRow)
//
//    //Формирование данных, которые не требуют изменения
////    val noActionRowsCondition =
////      col("increment_sha").isNull ||
////        col("increment_sha") === col("dm_sha")
//
//    val noActionDF = joinDF.
//      transform(filterDataByCondition(actionCondition.noActionRowsCondition)).
//      transform(getNoActionRows)
//
//    //Формирование данные, которые необходимо закрыть в витрине на день расчета
//    val upsertRowsCondition =
//      col("inc_mdm_id") === col("mdm_id") &&
//        col("increment_sha") =!= col("dm_sha")
//
//    val upsertDF = joinDF.
//      transform(filterDataByCondition(actionCondition.upsertRowsCondition)).
//      transform(getUpsertActionRows)
//
//    //Формирование новых данных из инкремента
//    val newRowsCondition = {
//      col("dm_sha").isNull ||
//        (
//          col("inc_mdm_id") === col("mdm_id") &&
//            col("increment_sha") =!= col("dm_sha")
//          )
//    }
//
//    val newRowsDF = joinDF.
//      transform(filterDataByCondition(actionCondition.newRowsCondition)).
//      transform(getNewActionRows)
//
//    //Формирование обновленной витрины с данными
//    val scd2DF = closedRowsDF.
//      transform(
//        unionDFs(
//          noActionDF,
//          upsertDF,
//          newRowsDF
//        )
//      )
//
//    scd2DF
//  }
//}
