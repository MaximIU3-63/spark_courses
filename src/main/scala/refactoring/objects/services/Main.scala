package refactoring.objects.services

import java.time.LocalDate

trait Date {
  val currentDate: String
}

class SystemDate extends Date {
  override lazy val currentDate: String = LocalDate.now().toString
}

class Logger {
  def log(currentDate: String): Unit = {
    println(s"[$currentDate] log info")
  }
}

object Main extends App {
  val logger = new Logger
  logger.log(new SystemDate().currentDate)
}
