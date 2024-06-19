package spark.datasets

import org.apache.spark.sql.Encoders
import spark.utils.ContextBuilder

object Customers extends App with ContextBuilder{

  override val appName: String = this.getClass.getSimpleName

  case class CustomersWithOutNulls(
                                    name: String,
                                    surname: String,
                                    age: Int,
                                    occupation: String,
                                    customer_rating: Double
                                  )

  case class CustomersWithNulls(
                                 name: String,
                                 age: Option[Int],
                                 customer_rating: Option[Double]
                               )

  import spark.implicits._

  val customersDS = spark.read.
    format("csv").
    schema(Encoders.product[CustomersWithOutNulls].schema).
    option("header", "True").
    option("sep", ",").
    load("src/main/resources/customers.csv").
    as[CustomersWithOutNulls]

  //customersDS.filter(s => s.age > 5 && s.occupation == "student").show

  val cDF = spark.read.
    format("csv").
    //schema(Encoders.product[CustomersWithNulls].schema).
    option("header", "True").
    option("sep", ",").
    load("src/main/resources/customers_with_nulls.csv")

  cDF.na.fill("n/a", List("age", "customer_rating")).show

  val customersWithNullsDS = spark.read.
    format("csv").
    schema(Encoders.product[CustomersWithNulls].schema).
    option("header", "True").
    option("sep", ",").
    load("src/main/resources/customers_with_nulls.csv").
    as[CustomersWithNulls]

  //customersWithNullsDS.filter(s => s.age.getOrElse(0) > 15).show

  spark.close()
}
