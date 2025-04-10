package spark.scd2.utils

/** Конфигуратор для описания партиции */
case class SCD2PartitioningConfig(colName: String, allowedValues: Seq[Int]) {
  require(allowedValues.nonEmpty, Messages.requireMessage("allowedValues"))
}

// Интерфейс для значений поля партицирования
sealed trait ActiveFlag {
  val value: AnyVal
}

// Формирование значения партиции для активных записей
case object Active extends ActiveFlag { val value = 1 }

// Формирование значения партиции для неактивных записей
case object Inactive extends ActiveFlag { val value = 0 }

object SCD2Defaults {
  val DefaultPartition: SCD2PartitioningConfig = SCD2PartitioningConfig(
    colName = "active_flg",
    allowedValues = Seq(Inactive.value, Active.value)
  )
}
