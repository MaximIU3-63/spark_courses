package refactoring.objects.practice

final case class Graduation(large: Double)

object Default {
  val InitialValue = 0.0
  val LargeLength = 1.80
}

object LengthConstants {
  val MeterLabel = "meter"
  val MeterAbbrev = "m"


  val CentimetreLabel = "centimetre"
  val CentimetreAbbrev = "cm"
}

sealed abstract class Category(val defaultUnit: MeasurementUnit)

final case object Length extends Category(Meter)

abstract class MeasurementUnit(
                                val label: String,
                                val abbreviation: String,
                                val factor: Double,
                                val category: Category,
                                val graduation: Graduation) {

  def initialValue: Double = Default.InitialValue

}

sealed abstract class LengthUnit(
                                  label: String,
                                  abbreviation: String,
                                  factor: Double)
  extends MeasurementUnit(
    label,
    abbreviation,
    factor,
    Length,
    Graduation(Default.LargeLength))

case object Meter extends LengthUnit(
  LengthConstants.MeterLabel,
  LengthConstants.MeterAbbrev,
  1.0)

case object Centimetre extends LengthUnit(
  LengthConstants.CentimetreLabel,
  LengthConstants.CentimetreAbbrev,
  0.01)

final case class Measurement private(
                                      id: Int,
                                      category: Category,
                                      measurementUnit: MeasurementUnit,
                                      value: Double) {

  def withUpdatedValue(newValue: Double): Measurement = {
    if (newValue < 0.0) throw new IllegalArgumentException(
      "Measurement value must be greater or  equal to zero")
    this.copy(value = newValue)
  }

  def withUpdatedUnit(newUnit: MeasurementUnit): Measurement = {
    this.copy(measurementUnit = newUnit)
  }
}


object Measurement {
  def apply(
             id: Int,
             category: Category,
             measurementUnit: MeasurementUnit,
             value: Double)
  : Measurement = {

    if (value < 0.0) throw new IllegalArgumentException(
      "Measurement value must be greater or equal to  zero")

    new Measurement(id, category, measurementUnit, value)
  }

  def apply(id: Int, category: Category): Measurement =
    new Measurement(
      id,
      category,
      category.defaultUnit,
      category.defaultUnit.initialValue
    )

}

class Estimator {
  private def convertToStandardUnit(value: Double, factor: Double): Double = value * factor


  private def isLarge(measurement: Measurement): Boolean = {
    convertToStandardUnit(
      measurement.value,
      measurement.measurementUnit.factor
    ) >= measurement.measurementUnit.graduation.large
  }

  def estimate(measurement: Measurement): Unit = {
    println(s"${measurement.value} ${measurement.measurementUnit.abbreviation} " +
      s"is large: ${isLarge(measurement)}")
  }

}
