package spark.practice.json

import io.circe.{Decoder, HCursor}
import io.circe.jawn.decode
import spark.utils.ContextBuilder

object AmazonProducts extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  val sc = spark.sparkContext

  private val jsonFileLocation: String = "src/main/resources/amazon_products.json"

  case class AmazonProducts(
                             uniqId: Option[String],
                             productName: Option[String],
                             manufacturer: Option[String],
                             price: Option[String],
                             numberAvailable: Option[String],
                             numberOfReviews: Option[Int]
                           )

  implicit val amazonProductsDecoder: Decoder[AmazonProducts] = (hCursor: HCursor) => {
    for {
      uniqId <- hCursor.get[Option[String]]("uniq_id")
      productName <- hCursor.get[Option[String]]("product_name")
      manufacturer <- hCursor.get[Option[String]]("manufacturer")
      price <- hCursor.get[Option[String]]("price")
      numberAvailable <- hCursor.get[Option[String]]("number_available")
      numberOfReviews <- hCursor.get[Option[Int]]("number_of_reviews")
    } yield AmazonProducts(uniqId, productName, manufacturer, price, numberAvailable, numberOfReviews)
  }

  private def decodeByRow(row: String): AmazonProducts = {
    val decoded = decode[AmazonProducts](row)
    decoded match {
      case Right(decodedRow) => decodedRow
    }
  }

  val amazonProductsRDD = sc.textFile(jsonFileLocation)

  val amazonProducts = amazonProductsRDD.
    map(decodeByRow)

  amazonProducts.foreach(println)

  spark.close()
}
