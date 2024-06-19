import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Row}

import java.nio.file.{FileSystems, Files, Path}
import scala.util.parsing.json._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source.fromFile

case class JsonConfigs(
                      database: String,
                      table: String,
                      joinCols: Array[String],
                      sensitiveCols: Array[String],
                      insensitiveCols: Array[String],
                      effectiveFromColName: String,
                      effectiveToColName: String,
                      dateFormat: String
                      )

class JsonExtractor {
  private val fileName: String = "scd2_traffic.json"
  private val path: String = s"src/main/resources/scd2/config"

  private implicit val formats: Formats = DefaultFormats

  def fullPath(): String = {
    path + "/" + fileName
  }

  private def arrayToSeqCol(array: Array[String]): Seq[Column] = {
    array.map(col).toSeq
  }

  def getJsonData: JsonConfigs = {
    val jsonContent = fromSource(fullPath()).mkString

    val json = parse(jsonContent).extract[JsonConfigs]
    json
  }
}

val trafficData: JsonConfigs = new JsonExtractor().getJsonData

println(trafficData)
