package refactoring.methods.practice.ex2

sealed trait ActivityFactors {
  val factor: Double
}

object Active extends ActivityFactors {
  override val factor: Double = 1.375
}

object Sedentary extends ActivityFactors {
  override val factor: Double = 1.2
}

case class Weight(value: Int, unit: String)
case class Height(value: Int, unit: String)

object Main extends App {
  private val person = Person(12, Weight(40, "kg"), Height(157, "sm"))
  private val calorieNeedsСalculator = CalorieNeedsCalculator()

  calorieNeedsСalculator.showCalorieNeededFor(person)
}

case class Person(
              age: Int,
              weight: Weight,
              height: Height
            ) {

  def getWeight: Int = weight.value
  def getHeight: Int = height.value
  def getAge: Int = age
}

//Сервис
case class CalorieNeedsCalculator() {

  private def normalizeDimensions(dimension: String): Int = {
    dimension.split(" ")(0).toInt
  }

  private def calculateBmr(weight: Int, height: Int, age: Int): Double = {
    66 + (13.7 * weight) + (5 * height) - (6.8 * age)
  }

  private def calculateCalorieNeeded(activityFactor: Double, person: Person): Double = {
    val bmr = calculateBmr(person.getWeight, person.getHeight, person.getAge)

    val calorieNeeds = bmr * activityFactor

    calorieNeeds
  }

  def showCalorieNeededFor(person: Person): Unit = {
    println(s"Needed calories: ${calculateCalorieNeeded(Active.factor, person)}")
  }

}