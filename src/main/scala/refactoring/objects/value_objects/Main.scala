package refactoring.objects.value_objects

/**************************/
/*     Value Object       */
/**************************/

/*
Cоздание отдельного объекта - значения Mobile для хранения данных о номере телефона
**
внедрив отдельный объект для хранения данных, прописав валидацию данных внутри этого объекта и гарантировав,
что данный объект будет содержать только проверенные данные, мы избавились от необходимости проверять данные каждый раз,
как возникнет необходимость работы с ними
**
 */

//value object
case class Mobile(operatorCode: String, number: String)

object Mobile {
  private def isValidNumber(operatorCode: String, number: String): Boolean = ???

  def apply(operatorCode: String, number: String): Mobile = {
    if(isValidNumber(operatorCode, number)) {
      new Mobile(operatorCode, number)
    } else throw new IllegalArgumentException()
  }
}

//entity
case class Customer(id: Int, mobile: Mobile) {
  def updateMobile(newMobile: Mobile): Customer = {
    Customer(this.id, newMobile)
  }
}


/*
-------ВНЕДРЕНИЕ ЗАВИСИМОСТЕЙ-------
Если мы работаем с объектом-сервисом, то для внедрения зависимостей используем аргументы конструктора.
Но если работа идет с объектом данных, то здесь должен быть использован другой подход:
объекты-данные не должны иметь зависимостей, указанных в качестве аргумента конструктора.
Вместо этого, если для выполнения задачи требуется сервис, то этот сервис указывается в качестве аргумента метода
(eще лучше будет, если будет указан не сам сервис, а получаемая от сервиса информация).
 */

//service
trait Translator {
  def translate(text: String): String
}

class EnglishTranslator extends Translator{
  override def translate(text: String): String = s"translated $text"
}

//value object
final case class Message(msg: String) {
  //service as a method argument
  def displayMsg(translator: Translator): Unit = {
    println(translator.translate(this.msg))
  }
}

object Main extends App {
  val mob = Mobile("495", "123456")

  val ivan = Customer(1, mob)
  ivan.updateMobile(Mobile("987", "234156"))
}
