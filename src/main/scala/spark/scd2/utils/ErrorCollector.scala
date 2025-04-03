package spark.scd2.utils

import scala.collection.mutable.ListBuffer

class ErrorCollector {
  // Инициализация буфера для коллекционирования ошибок работы функционала
  private val errorBuffer: ListBuffer[String] = new ListBuffer[String]()

  private lazy val getErrorsListAsString: String = errorBuffer.mkString("\n")

  def addError(errorMessage: String): Unit = {
    errorBuffer.append(errorMessage)
  }

  def throwIfErrorsExists(): Unit = {
    if(errorBuffer.nonEmpty) {
      throw new Exception(s"The following errors were found $getErrorsListAsString")
    }
  }
}
