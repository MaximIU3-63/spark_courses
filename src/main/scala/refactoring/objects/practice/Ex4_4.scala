package refactoring.objects.practice

import scala.util.matching.Regex

object ErrorMessages {
  val passwordMessage: String =
    """
      |Your password is not valid.
      |A password is considered valid if all the following constraints are satisfied:
      | - It contains at least 8 characters and at most 20 characters.
      | - It contains at least one digit.
      | - It contains at least one upper case alphabet.
      | - It contains at least one lower case alphabet.
      | - It contains at least one special character which includes !@#$%&*()-+=^.
      | - It doesn’t contain any white space
      |""".stripMargin
}

object ValidationPatterns {
  /** Паттерн для проверки валидности пароля.
   * Описание проверок:
   *
   * (?=.*[0-9])        содержит как минимум одной цифры.
   *
   * (?=.*[a-z])        содержит как минимум одну букву алфавита в нижнем регистре.
   *
   * (?=.*[A-Z])        содержит как минимум одну букву алфавита в верхнем регистре.
   *
   * (?=.*[@#$%&+=])    содержит как минимум один из специальных символов.
   *
   * (?=\\S+$).{8,20}$  длина пароля от 8 до 20 символов.
   */
  val passwordPattern: Regex = "^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[@#$%^&+=])(?=\\S+$).{8,20}$".r
}

final case class UserConfig(name: String, password: String) {
  require(name.nonEmpty && password.nonEmpty, "Name or password should not be empty.")
  require(isValidPassword, ErrorMessages.passwordMessage)

  private def isValidPassword: Boolean = {
    ValidationPatterns.passwordPattern findFirstMatchIn password match {
      case Some(_) => true
      case None => false
    }
  }
}

final case class Database(name: String) {
  require(name.nonEmpty, "Database name must be specified.")
}

final case class DbConnection(userConfig: UserConfig, database: Database)

object Ex4_4 extends App {
  val admin = UserConfig("admin", "123456rtyD!")
  val database = Database("mysql")

  val connection = DbConnection(admin, database)
}
