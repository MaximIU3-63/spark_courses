package spark.practice

import org.apache.spark.sql.{DataFrame, Encoders, SaveMode}
import org.apache.spark.sql.functions.{avg, col, lit, round, when}
import spark.utils.ContextBuilder

object MallCustomers extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  val fileLocation: String = "src/main/resources/mall_customers.csv"

  case class Customers(
                        customer_id: Int,
                        gender: String,
                        age: Int,
                        annual_income: Int,
                        spending_score: Int
                      )

  val mallCustomersSchema = Encoders.product[Customers].schema

  val mallCustomersDF = spark.read.
    format("csv").
    schema(mallCustomersSchema).
    options(
      Map(
        "multiLine" -> "true",
        "header" -> "true",
        "sep" -> ",",
        "path" -> fileLocation
      )
    ).load()

  def withProperAge(df: DataFrame): DataFrame = {
    df.withColumn("age", col("age").plus(2))
  }

  def withGenderCode(df: DataFrame): DataFrame = {
    val isMale = col("gender") === lit("Male")
    val isFemale = col("gender") === lit("Female")

    df.withColumn("gender_code",
      when(isMale, 1)
        .when(isFemale, 0)
        .otherwise(-1))
  }

  def extractCustomersGroups(df: DataFrame): DataFrame = {
    val columns = Seq(col("gender"), col("age"))
    val hasProperAge = col("age").between(30, 35)

    df.filter(hasProperAge).
      groupBy(columns: _*).
      agg(round(avg(col("annual_income")), 1).
        as("avg_annual_income")).
      orderBy(columns:_*)
  }

  val incomeDF = mallCustomersDF.
    transform(withProperAge).
    transform(extractCustomersGroups).
    transform(withGenderCode)

  try {
    incomeDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/customers")
  } catch {
    case ex: Throwable => println(ex.printStackTrace())
  } finally {
    spark.close()
  }
}
