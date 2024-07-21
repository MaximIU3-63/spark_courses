package refactoring.objects.practice

trait Converter {
  def convert(value: String): String
}

class CountryCodeConverter extends Converter {
  override def convert(value: String): String = {
    value match {
      case "RU" => "Russia"
      case "CA" => "Canada"
      case _ => "Unknown"
    }
  }
}

//Объект с данными
case class User(id: Int, username: String, password: String, countryCode: String) {
  //Передаем сервис в качестве арргумента метода
  def getCountry(converter: Converter): String = {
    val countryName = converter.convert(this.countryCode)
      countryName
  }
}

object Exercise_2 extends App {
  val converter = new CountryCodeConverter
  val user = User(1, "Maksim", "123", "RU")
  println(user.getCountry(converter))
}
