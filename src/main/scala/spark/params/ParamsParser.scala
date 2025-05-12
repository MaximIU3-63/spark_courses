package spark.params

import scala.collection.mutable.{Map => MutableMap}


object ParamsParser {
  def main(args: Array[String]): Unit = {
    val argsForTest = Array(
        "--source",
        "table1=q",
        "table2=w",
        "table3-r",
        "--target",
        "table11=tq",
        "table12=tw",
        "table13=tr",
        "--additional",
        "path=/data/regular/"
    )

    val config = parseArgs(argsForTest)
    println(s"Sources: ${config.sources}")
    println(s"Targets: ${config.targets}")
    println(s"Additional: ${config.additional}")
  }

  case class AppConfig(
                        sources: Map[String, String],
                        targets: Map[String, String],
                        additional: Map[String, String]
                      )

  def parseArgs(args: Array[String]): AppConfig = {

    val sources = MutableMap[String, String]()
    val targets = MutableMap[String, String]()
    val additional = MutableMap[String, String]()
    var currentSection: Option[String] = None

    def processEntry(entry: String, section: String): Unit = {
      entry.split("[=:-]", 2) match {
        case Array(key, value) =>
          val cleanedKey = key.trim
          val cleanedValue = value.trim
          section match {
            case "source" => sources += (cleanedKey -> cleanedValue)
            case "target" => targets += (cleanedKey -> cleanedValue)
            case "additional" => additional += (cleanedKey -> cleanedValue)
          }
        case _ =>
          System.err.println(s"Invalid format for entry: $entry")
      }
    }

    args.foreach {
      case "--source" => currentSection = Some("source")
      case "--target" => currentSection = Some("target")
      case "--additional" => currentSection = Some("additional")
      case entry if currentSection.isDefined =>
        println(s"entry - $entry, currentSection - $currentSection")
        processEntry(entry, currentSection.get)
      case other =>
        System.err.println(s"Ignoring unrecognized argument: $other")
    }

    AppConfig(sources.toMap, targets.toMap, additional.toMap)
  }
}