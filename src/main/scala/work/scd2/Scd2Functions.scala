//package work.scd2
//
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{Column, DataFrame}
//import .withColumn
//
//object Scd2Functions {
//
//  def toSeqCol(array: List[String]): Seq[Column] = {
//    array.map(col)
//  }
//
//  //Объединение датафреймов с разным типом действия
//  def unionDFs(noActionDF: DataFrame,
//                       upsertDF: DataFrame,
//                       newRowsDF: DataFrame)(closedDF: DataFrame): DataFrame = {
//    closedDF.
//      union(noActionDF).
//      union(upsertDF).
//      union(newRowsDF)
//  }
//
//  //Фильтрация данных. Отбор открытых строк, где effective_to_dt = 9999-12-31
//  def isOpenRow(dtColName: Column)(df: DataFrame): DataFrame = {
//    df.filter(
//      to_date(dtColName) === "2999-12-31"
//    )
//  }
//
//  //Фильтрация данных. Отбор закрытых строк, где effective_to_dt < текущей даты
//  def isClosedRow(dtColName: Column)(df: DataFrame): DataFrame = {
//    df.filter(
//      to_date(dtColName) < current_date()
//    )
//  }
//
//  //Функция по созданию колонки с хэш суммой в датафрейме по определенным полям
//  def withSha1Column(colName: String, colsToSha: Seq[Column])(df: DataFrame): DataFrame = {
//    val condition = sha1(concat_ws("salt", colsToSha: _*))
//    df.
//      transform(withColumn(colName, condition))
//  }
//
//  //Join типа FULL между витриной и инкрементом по ключевому полю mdm_id -> inc_mdm_id
//  def joinFullDF(incrementDF: DataFrame, cond: Column)(dmDF: DataFrame): DataFrame = {
//    dmDF.
//      join(incrementDF, cond, "full")
//  }
//}
