package spark.practice.datasets

import org.apache.spark.sql.functions.{coalesce, col, lit, max, min, regexp_replace, split, sum}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders}
import spark.utils.ContextBuilder

object AiJobIndustry extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  private val fileLocation: String = "src/main/resources/AiJobsIndustry.csv"

  case class JobIndustry(
                          jobTitle: String,
                          company: String,
                          location: String,
                          companyReviews: String,
                          link: String
                        )

  def read(location: String, format: String = "csv"): DataFrame = {
    spark.read.
      format(format).
      options(
        Map(
          "header" -> "true",
          "sep" -> ",",
          "path" -> location,
          "multiLine" -> "true"
        )
      ).
      schema(Encoders.product[JobIndustry].schema).
      load()
  }

  def write(df: DataFrame): Unit = {
    df.
      repartition(10).
      write.
      mode("overwrite").
      option("header", "true").
      csv("src/main/resources/data/ai/")
  }

  def withUpdateColumnByRegExp(
                                colName: String,
                                updateValue: Column,
                                pattern: String,
                                replacement: String = ""
                              )(df: DataFrame): DataFrame = {
    df.
      withColumn(colName,
        regexp_replace(updateValue, pattern, replacement))
  }

  def withColumnLinkUniqParameter(uniqCol: String)(df: DataFrame): DataFrame = {
    val split_col = split(col("link"), "[\\[\\]]").
      getItem(1)

    df.withColumn(uniqCol, split_col).
      dropDuplicates(uniqCol)
  }

  def withCompanyReviewsCast(df: DataFrame): DataFrame = {
    df.withColumn("companyReviews", col("companyReviews").cast("int"))
  }

  def replaceNull(colName: String, column: Column = lit("n/a")): Column = {
    coalesce(col(colName), column).as(colName)
  }

  def extractWithReplaceNulls(df: DataFrame): DataFrame = {
    df.select(
      replaceNull("jobTitle"),
      replaceNull("company"),
      replaceNull("location"),
      replaceNull("companyReviews", lit("0")),
      replaceNull("link")
    )
  }

  def extractColumns(df: DataFrame): DataFrame = {
    df.
      transform(extractWithReplaceNulls).
      transform(withUpdateColumnByRegExp("company", col("company"), "[\\n    ]")).
      transform(withUpdateColumnByRegExp("companyReviews", col("companyReviews"), "\\D+")).
      transform(withColumnLinkUniqParameter("link_parameters")).
      transform(withCompanyReviewsCast)
  }

  def sumReviewsByCol(groupByCol: String, sumCol: String)(df: DataFrame): DataFrame = {
    df.
      groupBy(groupByCol).
      agg(sum(col("companyReviews")).as(sumCol))
  }

  def getReviewsStatisticByLocation(
                                        groupByCol: String,
                                        locationCol: String,
                                        statsType: String,
                                        countCol: String,
                                        countType: String
                                      )(df: DataFrame): DataFrame = {
    countType match {
      case "max" => df.
        groupBy(groupByCol, locationCol).
        agg(max(col("companyReviews")).as(countCol)).
        orderBy(col(countCol).desc).
        limit(1).
        select(
          col(groupByCol).as("name"),
          lit(statsType).as("stats_type"),
          col(locationCol),
          col(countCol).as("count"),
          lit(countType).as("count_type")
        )
      case "min" => df.
        groupBy(groupByCol, locationCol).
        agg(min(col("companyReviews")).as(countCol)).
        orderBy(col(countCol).asc).
        limit(1).
        select(
          col(groupByCol).as("name"),
          lit(statsType).as("stats_type"),
          col(locationCol),
          col(countCol).as("count"),
          lit(countType).as("count_type")
        )
    }

  }

  def unionReviewsStatistic(df: DataFrame): DataFrame = {
    df.
      transform(getReviewsStatisticByLocation("jobTitle", "location", "job", "count", "max")).
      union(
        df.
          transform(getReviewsStatisticByLocation("jobTitle", "location", "job", "count", "min"))
      ).
      union(
        df.
          transform(getReviewsStatisticByLocation("company", "location", "company", "count", "max"))
      ).
      union(
        df.
          transform(getReviewsStatisticByLocation("company", "location", "company", "count", "min"))
      )
  }

  def extractReviewsStatistic(df: DataFrame): DataFrame = {
    df.
      transform(unionReviewsStatistic)
  }

  val jobsDF: DataFrame = read(fileLocation).
    transform(extractColumns)

  //Общее количество отзывов по каждой компании
  val reviewsCountByCompanyDF = jobsDF.
    transform(sumReviewsByCol("company", "reviews_count"))

  //Общее количество отзывов по каждой вакансии
  val reviewsCountByJobTitleDF = jobsDF.
    transform(sumReviewsByCol("jobTitle", "reviews_count"))

  val jobsStatsDF = jobsDF.transform(extractReviewsStatistic)

  jobsStatsDF.show

  jobsStatsDF.explain("formatted")

  spark.close()
}
