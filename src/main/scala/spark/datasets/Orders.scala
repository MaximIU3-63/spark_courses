package spark.datasets

import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row}
import spark.utils.ContextBuilder

object Orders extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  case class Order(
                    orderId: Int,
                    customerId: Int,
                    product: String,
                    quantity: Int,
                    priceEach: Double
                  )

  val ordersData: Seq[Row] = Seq(
    Row(1, 2, "USB-C Charging Cable", 3, 11.29),
    Row(2, 3, "Google Phone", 1, 600.33),
    Row(2, 3, "Wired Headphones", 2, 11.90),
    Row(3, 2, "AA Batteries (4-pack)", 4, 3.85),
    Row(4, 3, "Bose SoundSport Headphones", 1, 99.90),
    Row(4, 3, "20in Monitor", 1, 109.99)
  )

  val ordersSchema = Encoders.product[Order].schema

  import spark.implicits._

  val ordersDataDS = spark.createDataFrame(
   spark.sparkContext.parallelize(ordersData),
    ordersSchema
  ).as[Order]

  def getTotalStats(order: Dataset[Order]): (Double, Int) = {
    val stats = order.map({
      order => (order.priceEach * order.quantity, order.quantity)
    }).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    (stats._1, stats._2)
  }
  val (price, orderQuantity) = getTotalStats(ordersDataDS)

  println(price, orderQuantity)

  case class CustomerInfo(
                           customerId: Int,
                           priceTotal: Double
                         )

  val infoDS: Dataset[CustomerInfo] = ordersDataDS.
    groupByKey(_.customerId).
    mapGroups({
      (id, orders) => {
        val priceTotal = orders.map(order => order.priceEach * order.quantity).sum.round

        CustomerInfo(
          id, priceTotal
        )
      }
    })

  infoDS.show

  //////Exercise 3//////
  case class Sales(
                    customer: String,
                    product: String,
                    price: Double,
                  )

  case class Customer(
                       id: Int,
                       email: String,
                       orders: Seq[Int])

  case class Order_(
                    orderId: Int,
                    product: String,
                    quantity: Int,
                    priceEach: Double)


  val customerData: Seq[Row] = Seq(
    Row(1, "Bob@example.com", Seq()),
    Row(2, "alice@example.com", Seq(1, 3)),
    Row(3, "Sam@example.com", Seq(2, 4))
  )

  val ordersData_2: Seq[Row] = Seq(
    Row(1, "USB-C Charging Cable", 3, 11.29),
    Row(2, "Google Phone", 1, 600.33),
    Row(2, "Wired Headphones", 2, 11.90),
    Row(3, "AA Batteries (4-pack)", 4, 3.85),
    Row(4, "Bose SoundSport Headphones", 1, 99.90),
    Row(4, "20in Monitor", 1, 109.99)
  )

  def toDS[T <: Product: Encoder](data: Seq[Row], schema: StructType): Dataset[T] = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    ).as[T]
  }

  import spark.implicits._

  val customersDS = toDS[Customer](customerData, Encoders.product[Customer].schema)
  val ordersDS = toDS[Order_](ordersData_2, Encoders.product[Order_].schema)

  val joinedDS = customersDS.joinWith(ordersDS, array_contains(customersDS.col("orders"), ordersDS.col("orderId")), "outer")

  joinedDS.show(false)

  case class Sales_(
                    customer: String,
                    product: String,
                    price: Double,
                  )

  val salesDS = joinedDS.filter(row => row._1.orders.nonEmpty).
    map({
      case (order, customer) => Sales(order.email.toLowerCase(), customer.product, customer.quantity * customer.priceEach)
    })

  val salesDS_2 = joinedDS.map({
    case (order, customer) => if (order.orders.isEmpty) {
      Sales(
        order.email.toLowerCase(),
        "-",
        0.0
      )
    } else {
      Sales(order.email.toLowerCase(),
        customer.product,
        customer.quantity * customer.priceEach)
    }
  })

  salesDS.show
  salesDS_2.show

  spark.close()
}
