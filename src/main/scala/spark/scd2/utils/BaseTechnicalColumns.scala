package spark.scd2.utils

//Трейт с описанием базовых технических полей
trait BaseTechnicalColumns {
  val sysDateColumnName: String
  val sysDateDttmColumnName: String
}
