import scala.util.matching.Regex

object ErrorMessages {
  val unspecifiedDb: String = "Database name must be specified."
  val emptyNameOrPassword = "Name or password should not be empty."
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
  require(name.nonEmpty && password.nonEmpty, ErrorMessages.emptyNameOrPassword)
  require(isValidPassword, ErrorMessages.passwordMessage)

  private def isValidPassword: Boolean = {
    ValidationPatterns.passwordPattern findFirstMatchIn password match {
      case Some(_) => true
      case None => false
    }
  }
}

final case class Database(name: String) {
  require(name.nonEmpty, ErrorMessages.unspecifiedDb)
}

final case class DbConnection(userConfig: UserConfig, database: Database)