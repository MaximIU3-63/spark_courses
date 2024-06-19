package work

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source.fromFile

object JsonReader {
  private implicit val formats: Formats = DefaultFormats

  def readJsonFile(filePath: String): JsonConfigs = {
    val jsonContent = fromFile(filePath).mkString
    parse(jsonContent).extract[JsonConfigs]
  }
}

case class File(
               name: String,
               path: String = "src/main/resources/scd2/config"
               ) {
  lazy val fullPath: String = s"$path/$name"
}

object Main extends App {

  private val trafficJsonFile = File("scd2_traffic.json")

  val trafficData = JsonReader.readJsonFile(trafficJsonFile.fullPath)

  println(trafficData)
}
