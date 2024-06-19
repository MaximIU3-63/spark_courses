package spark.practice

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, count, explode, lit, lower, monotonically_increasing_id}
import spark.utils.ContextBuilder

import scala.language.implicitConversions

object DfColumn extends DfColumn {
  implicit def columnToString(col: DfColumn.Value): String = col.toString
}

trait DfColumn extends Enumeration {
  val _c0, id,
  w_s1, w_s2,
  cnt_s1, cnt_s2 = Value
}

object Subtitles extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  val fileLocationSubtitlesS1: String = "src/main/resources/subtitles_s1.json"
  val fileLocationSubtitlesS2: String = "src/main/resources/subtitles_s2.json"

  def read(file: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .csv(file)
  }

  val subtitlesS1DF: DataFrame = read(fileLocationSubtitlesS1)
  val subtitlesS2DF = read(fileLocationSubtitlesS2)

  import spark.implicits._

  def extractWords(wordCol: String)(df: DataFrame): DataFrame = {
    val isNotEmpty = col(wordCol) =!= lit("")
    val isNotNum = col(wordCol).cast("int").isNull

    df.
      flatMap(s => s.mkString.toLowerCase().split("\\W+")).
      toDF(wordCol).
      filter(isNotEmpty && isNotNum)
  }

  def groupByWord(wordCol: String, cntCol: String)(df: DataFrame): DataFrame = {
    val column = col(wordCol)

    df.
      groupBy(column).
      agg(count(column).as(cntCol))
  }

  def topTwentyWordsByCount(cntCol: String)(df: DataFrame): DataFrame = {
    df.
      orderBy(col(cntCol).desc).
      limit(20)
  }

  def withId(df: DataFrame): DataFrame = {
    df.withColumn(DfColumn.id, monotonically_increasing_id())
  }

  def extractTop20Words(wordCol: String, cntCol: String)(df: DataFrame): DataFrame = {
    df.
      transform(extractWords(wordCol)).
      transform(groupByWord(wordCol, cntCol)).
      transform(topTwentyWordsByCount(cntCol)).
      transform(withId)
  }

  def joinDF(s2DF: DataFrame)(s1DF: DataFrame): DataFrame = {
    val joinCol = Seq(DfColumn.id.toString)

    s1DF.join(s2DF, joinCol, "inner")
  }

  val mostPopularWordsS1DF = subtitlesS1DF.transform(extractTop20Words(DfColumn.w_s1, DfColumn.cnt_s1))
  val mostPopularWordsS2DF = subtitlesS2DF.transform(extractTop20Words(DfColumn.w_s2, DfColumn.cnt_s2))

  val mostPopularWordsDF = mostPopularWordsS1DF.transform(joinDF(mostPopularWordsS2DF))

  try {
    mostPopularWordsDF.
      write.
      mode(SaveMode.Overwrite).
      json("src/main/resources/data/wordcount")
  } catch {
    case ex: Throwable => println(ex.printStackTrace())
  } finally {
    spark.close()
  }
}
