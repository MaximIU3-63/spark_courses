package refactoring.methods.practice.ex1

object Values {
  val currentYear = 2023
  val currentMonth = 11
}

object Student extends App {
  val bob = new Student("Bob", "2003/01/20", 2020)
  bob.printInfo()
}

class Student(name: String, birth: String, enrollmentYear: Int) {

  def printUserDetails(name: String, age: Int, course: Int): Unit = {
    println(s"Name: [$name]\n Age: [$age]\nCourse: [$course]")
  }

  def calculateCourse(currentYear: Int, enrollmentYear: Int): Int = currentYear - enrollmentYear

  def calculateBirthMonth(): Int = birth.split("/")(1).toInt

  def calculateBirthYear(): Int = birth.split("/")(0).toInt

  def calculateYear(currentMonth: Int, currentYear: Int): Int = {
    val year = if (calculateBirthMonth() > currentMonth) {
      currentYear - 1
    } else currentYear

    year
  }

  def calculateAge(currentMonth: Int, currentYear: Int): Int = {
    val year = calculateYear(currentMonth, currentYear)
    val birthYear = calculateBirthYear()
    val age = year - birthYear
    age
  }

  // метод, рефакторинг которого вы выполняете
  def printInfo(): Unit = {
    val course = calculateCourse(Values.currentYear, enrollmentYear)
    val age = calculateAge(Values.currentMonth, Values.currentYear)

    printUserDetails(name, age, course)
  }
}