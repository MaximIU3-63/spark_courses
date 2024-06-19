package spark.practice.optimizations

import org.apache.spark.sql.functions.{avg, col, concat, floor, lit, rand, sha1}
import org.apache.spark.sql.{DataFrame, Encoders, functions}
import org.apache.spark.sql.types.StructType
import spark.utils.ContextBuilder

import java.time.Instant

object FreelancersOffersStats extends App with ContextBuilder{
  override val appName: String = this.getClass.getSimpleName

  val freelancersFileLocation: String = "src/main/resources/freelancers.csv"
  val offersFileLocation: String = "src/main/resources/offers.csv"

  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  case class Freelancer(
                         id: String,
                         category: String,
                         city: String,
                         experienceLevel: Int
                       )

  case class Offer(
                    category: String,
                    city: String,
                    experienceLevel: Int,
                    price: Double
                  )

  def readCsv(file: String, schema: StructType): DataFrame = {
    spark.read.
      options(
        Map(
          "header" -> "true",
          "multiLine" -> "true"
        )
      ).
      schema(schema).
      csv(file)
  }

  def withSaltColumn(newColName: String)(df: DataFrame): DataFrame = {
    df.
      withColumn(newColName, concat(col("category"), col("city"), lit("_"), (rand() * lit(20)).cast("int")))
  }

  val freelancersSchema = Encoders.product[Freelancer].schema
  val offersSchema = Encoders.product[Offer].schema

  val freelancersDF = readCsv(freelancersFileLocation, freelancersSchema)

  val offersDF = readCsv(offersFileLocation, offersSchema)

//  freelancersDF.groupBy(col("city")).
//    agg(functions.count(col("city")).as("cnt")).
//    orderBy(col("cnt").desc).
//    show(100)
//
//  freelancersDF.groupBy(col("category")).
//    agg(functions.count(col("category")).as("cnt")).
//    orderBy(col("cnt").desc).
//    show(100)
//
//  offersDF.groupBy(col("city")).
//    agg(functions.count(col("city")).as("cnt")).
//    orderBy(col("cnt").desc).
//    show(100)
//
//  offersDF.groupBy(col("category")).
//    agg(functions.count(col("category")).as("cnt")).
//    orderBy(col("cnt").desc).
//    show(100)

//  val freelancerShaDF = freelancersDF.
//    transform(withSaltColumn("salt_category", "category")).
//    transform(withSaltColumn("salt_city", "city"))
//
//  freelancerShaDF.groupBy(col("salt_city")).
//    agg(functions.count(col("salt_city")).as("cnt")).
//    orderBy(col("cnt").desc).
//    show(100)
//
//  freelancerShaDF.groupBy(col("salt_category")).
//    agg(functions.count(col("salt_category")).as("cnt")).
//    orderBy(col("cnt").desc).
//    show(100)

  val start = Instant.now().getEpochSecond

  freelancersDF.join(offersDF, Seq("category", "city"))
    .filter(functions.abs(freelancersDF.col("experienceLevel") - offersDF.col("experienceLevel")) <= 1)
    .groupBy("id")
    .agg(avg("price").as("avgPrice"))
    .orderBy(col("id").desc)
    .show()

  println(Instant.now().getEpochSecond - start)

  /////////////////////////////////
  val freelancerShaDF = freelancersDF.
    transform(withSaltColumn("salt_col"))

  val offersShaDF = offersDF.
    transform(withSaltColumn("salt_col"))

  val start1 = Instant.now().getEpochSecond

  freelancerShaDF.join(offersShaDF, Seq("salt_col"), "inner")
    .filter(functions.abs(freelancerShaDF.col("experienceLevel") - offersShaDF.col("experienceLevel")) <= 1)
    .groupBy("id")
    .agg(avg("price").as("avgPrice"))
    .orderBy(col("id").desc)
    .show()

  println(Instant.now().getEpochSecond - start1)
  spark.close()
}
