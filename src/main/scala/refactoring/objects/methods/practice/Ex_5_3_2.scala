package refactoring.objects.methods.practice


//Сокращенные наименования единиц измерения
object MeasurementConstants {
  val MeterAbbrev = "m"
  val CentimetreAbbrev = "cm"
  val KilometerAbbrev = "km"
}

/** Абстрактный класс для определения единиц измерения
 *
 * @param factor - фактор единицы измерения
 *
 * За эталон берется значение фактора единицы измерения метр, который равен 1.
 *
 * Фактор для каждой новой единицы измерения длины указывается исходя из расчета, сколько метров в одной добавляемой единице измерения.
 */
abstract class Units(val factor: Double, val abbreviate: String) {
  require(factor > 0.0, "The value of the factor parameter cannot be negative")
}

case object Centimeter extends Units(0.01, MeasurementConstants.CentimetreAbbrev)
case object Meter extends Units(1.0, MeasurementConstants.MeterAbbrev)
case object Kilometer extends Units(1000.0, MeasurementConstants.KilometerAbbrev)

trait Category
case object Length extends Category
case object Height extends Category

final case class Measurement(
                            id: Int,
                            category: Category,
                            value: Double,
                            unit: Units
                            ) {
  require(value > 0.0, "The value of the measurement value cannot be negative")
}

final class MeasurementConverter {
  def convert(measurement: Measurement, toUnit: Units): Measurement = {
    if(measurement.unit.factor < toUnit.factor || measurement.unit.factor > toUnit.factor) {
      val newValue = Math.round(measurement.value * measurement.unit.factor / toUnit.factor)
      measurement.copy(value = newValue, unit = toUnit)
    } else {
      measurement
    }
  }
}

object Ex_5_3_2 extends App {
  val converter = new MeasurementConverter

  val length = Measurement(1, Length, -2, Kilometer)
  val lengthResult: Measurement = converter.convert(length, Meter)
  println(s"Result: ${length.value} ${length.unit.abbreviate}  = ${lengthResult.value} ${lengthResult.unit.abbreviate}")

  //val height = Measurement(1, Height, 259.34, Meter)
  val heightResult = converter.convert(lengthResult, Centimeter)
  println(s"Result: ${lengthResult.value} ${lengthResult.unit.abbreviate}  = ${heightResult.value} ${heightResult.unit.abbreviate}")
}
