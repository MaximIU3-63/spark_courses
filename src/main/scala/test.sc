import org.apache.spark.sql.{Column, DataFrame, SparkSession}
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.{Column, DataFrame, Row}
//
//import java.nio.file.{FileSystems, Files, Path}
//import scala.util.parsing.json._
//import org.json4s._
//import org.json4s.jackson.JsonMethods._
//
//import scala.io.Source.fromFile
//
//case class JsonConfigs(
//                      database: String,
//                      table: String,
//                      joinCols: Array[String],
//                      sensitiveCols: Array[String],
//                      insensitiveCols: Array[String],
//                      effectiveFromColName: String,
//                      effectiveToColName: String,
//                      dateFormat: String
//                      )
//
//class JsonExtractor {
//  private val fileName: String = "scd2_traffic.json"
//  private val path: String = s"src/main/resources/scd2/config"
//
//  private implicit val formats: Formats = DefaultFormats
//
//  def fullPath(): String = {
//    path + "/" + fileName
//  }
//
//  private def arrayToSeqCol(array: Array[String]): Seq[Column] = {
//    array.map(col).toSeq
//  }
//
//  def getJsonData: JsonConfigs = {
//    val jsonContent = fromSource(fullPath()).mkString
//
//    val json = parse(jsonContent).extract[JsonConfigs]
//    json
//  }
//}
//
//val trafficData: JsonConfigs = new JsonExtractor().getJsonData
//
//println(trafficData)

trait DataFrameReader {
  def read(tableName: String): DataFrame
}

object DataFrameTableReader {
  case class Config(tableName: String,
                    columns: Seq[Column],
                    isDistinct: Boolean = false)
}

final class DataFrameTableReader(spark: SparkSession, config: DataFrameTableReader.Config) extends DataFrameReader {
  override def read(tableName: String): DataFrame = {
    if(config.isDistinct) {
      spark.table(config.tableName).
        select(config.columns: _*)
    } else {
      spark.table(config.tableName).
        select(config.columns: _*).
        distinct()
    }
  }
}


trait DataFrameTransformer {
  def transform(inputDF: DataFrame, inputDFs: DataFrame*): Unit
}

object DataFrameTransformerFactory {
  /**
   * @param transformerClass the fully qualified class name of the transformer to instantiate
   * @return an instance of transformer of the given class
   */
  def getTransformer(transformerClass: String): DataFrameTransformer = {
    getClass.getClassLoader.loadClass(transformerClass).newInstance().asInstanceOf[DataFrameTransformer]
  }
}

final class TransformDmClient extends DataFrameTransformer {
  override def transform(inputDF: DataFrame, inputDFs: DataFrame*): Unit = {
    require()
  }
}