package refactoring.objects.practice

/*
class Length(var value: Double, var unit: String) {

     def this() = this(null, "")

     def changeValue(value: Double): Unit = {
      this.value = value
     }

     def changeUnit(unit: String): Unit = this.unit = unit

     def isLarge(): Boolean = {
       if (value >= 180.0 && unit == "meter") true
       else false
      }
  }


  val a = new Length()
  println(a.value)
 */

/**
 * Меры измерения длины
 *
 * Предполагается использование различных единиц измерения
 */
trait Measurement {
  val unit: String
}

object Meter extends Measurement {
  override val unit: String = "meter"
}

/*
Объект-компаньон для класса Length с реализацией конструкторов
 */
object Length {
  def apply(): Length = new Length(0.0, "undefined")
  def apply(value: Double, measurement: String): Length = new Length(value, measurement)
}

final case class Length private(value: Double, measurement: String) {
  def withChangedValue(newValue: Double): Length = {
    Length(newValue, measurement)
  }

  def withChangedMeasurement(newMeasurement: String): Length = {
    Length(value, newMeasurement)
  }

  def isLargeThan(comparisonValue: Double, comparisonMeasurement: String): Boolean = {
    if(measurement != comparisonMeasurement) {
      throw new IllegalArgumentException("Different measurements are compared")
    } else if(value > comparisonValue) {
      true
    } else {
      false
    }
  }
}

/**
 * Эталонное решение
 */



object Exercise_1 extends App {
  val length = Length(10.0, "meter")
  println(length.withChangedValue(34.0))
  println(length.withChangedMeasurement("cm"))
  println(length.isLargeThan(9.0, "cm"))
}
