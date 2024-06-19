package refactoring.objects.classes

//1 Выделение отдельного класса

/*
class Employee(
      id: Int,
      name: String,
      city: String,
      street: String,
      apartment: String,
      phoneNumber: String) {

    def displayAddress(): Unit = {
      println(
        s"Address of $name:\n" +
        s"$city $street $apartment")
    }
  }
 */

class Employee(
                id: Int,
                name: String,
                address: Address,
                phoneNumber: String) {
  def displayAddress(): Unit = {
    println(
      s"Address of $name:\n" +
        s"${address.fullAddress()}")
  }
}

case class Address(
                    city: String,
                    street: String,
                    apartment: String
                  ) {
  def fullAddress() = s"$city, $street, $apartment."
}

//2 Объединение классов
/*
class Employee(personalData: PersonalData) {
    def displayFullName(): Unit = println(personalData)
  }

  case class PersonalData(name: String, surname: String) {
    override def toString: String = name + " " + surname
  }
 */

class Employee_2(name: String, surname: String) {
  def displayFullName(): Unit = println(name + " " + surname)
}

object Main extends App
