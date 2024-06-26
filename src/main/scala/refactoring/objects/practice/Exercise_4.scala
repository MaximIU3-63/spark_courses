package refactoring.objects.practice

object FileFormat extends Enumeration {
  type FileFormat = Value

  val CSV = Value("csv")
}

final case class Config(fileName: String, fileFormat: String)

trait Reader {
  def read(file: String, fileFormat: String): Unit
}

/*
Сервис по считыванию csv файлов
 */
final class CsvReader extends Reader {

  private def isValidFormat(fileFormat: String): Boolean = {
    if(fileFormat.toLowerCase() == FileFormat.CSV.toString) true
    else false
  }

  override def read(file: String, fileFormat: String): Unit = {
    if(isValidFormat(fileFormat)) {
      println(s"read file [$file]")
    }
    else {
      throw new IllegalArgumentException("Bad file format")
    }
  }
}

object Exercise_4 extends App {
  val csvConfig = Config("file", FileFormat.CSV.toString)

  val csvReader = new CsvReader()

  csvReader.read(csvConfig.fileName, csvConfig.fileFormat)
}
