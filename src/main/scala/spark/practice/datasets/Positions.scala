package spark.practice.datasets

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import spark.utils.ContextBuilder

object Positions extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  private val fileLocation: String = "src/main/resources/hrdataset.csv"

  val REQUEST: List[String] = List("BI", "it")

  /*
  Версия кода с использование только DataSet.
  */
  case class PositionFullDS(
                             employee_name: String,
                             empId: Int,
                             marriedId: Int,
                             maritalStatusId: Int,
                             genderId: Int,
                             empStatusId: Int,
                             deptId: Int,
                             perfScoreId: Int,
                             fromDiversityJobFairId: Int,
                             salary: Int,
                             termd: Int,
                             positionId: Int,
                             position: String,
                             state: String,
                             zip: String,
                             dob: String,
                             sex: String,
                             maritalDesc: String,
                             citizenDesc: String,
                             hisPaniclatino: String,
                             raceDesc: String,
                             dateOfHire: String,
                             dateOfTermination: Option[String],
                             termReason: String,
                             employmentStatus: String,
                             department: String,
                             managerName: String,
                             managerId: Option[Int],
                             recruitmentSource: String,
                             performanceScore: String,
                             engagementSurvey: Double,
                             empSatisfaction: Int,
                             specialProjectsCount: Int,
                             lastPerformanceReview_date: String,
                             daysLateLast30: Int,
                             absences: Int
                           )

  case class PositionAfterRequest(
                                   positionId: Int,
                                   position: String,
                                 )

  /*
  Проверяется, что значения столбца position начинаются на значения из запроса.
  Запрос представляется в виде списка строк.
   */
  import spark.implicits._

  def extractCols(ds: Dataset[PositionFullDS]): Dataset[PositionAfterRequest] = {
    ds.
      map(row => PositionAfterRequest(row.positionId, row.position)).
      distinct()
  }

  def isCorrespondingPosition(request: List[String])(ds: Dataset[PositionAfterRequest]): Dataset[PositionAfterRequest] = {
    ds.
      filter(rec => {
        request.
          exists(r => rec.position.toLowerCase.startsWith(r.toLowerCase))
      })
  }

  def readAsDS[T <: Product : Encoder](location: String, schema: StructType): Dataset[T] = {
    spark.read.
      options(
        Map(
          "header" -> "true",
          "sep" -> ",",
          "multiLine" -> "true",
          "path" -> location
        )).
      schema(schema).
      csv().
      as[T]
  }

  val positionFullDSSchema: StructType = Encoders.product[PositionFullDS].schema

  val positionFullDS: Dataset[PositionFullDS] = readAsDS[PositionFullDS](fileLocation, positionFullDSSchema)

  val positionAfterRequestDS: Dataset[PositionAfterRequest] = positionFullDS.
    transform(extractCols).
    transform(isCorrespondingPosition(REQUEST))

  positionAfterRequestDS.show()

  /*
  Версия кода с использование излечения необходимых столбцов и дальнейщее преобразование в DataSet.
  Позволяет не концентрироваться на анализе всех столбцов файла, их данных и типа данных, а только на
  тех, с которыми предстоит работать.
   */

  def read(location: String): DataFrame = {
    spark.read.
      options(
        Map(
          "header" -> "true",
          "sep" -> ",",
          "multiLine" -> "true",
          "path" -> location
        )).
      csv()
  }

  /*
  Извлечение столбцов positionId, position и дедубликация и преобразование к нужному типу.
   */
  def extractPositionsWithIdDistinct(df: DataFrame): DataFrame = {
    df.
      select(
        col("positionId").cast("int"),
        col("position").cast("string")
      ).distinct()
  }

  val positionsDS: Dataset[PositionAfterRequest] = read(fileLocation).
    transform(extractPositionsWithIdDistinct).
    as[PositionAfterRequest]

  val positionsAfterRequestDS: Dataset[PositionAfterRequest] = positionsDS.
    transform(isCorrespondingPosition(REQUEST))

  positionsAfterRequestDS.show()

  spark.close()
}
