package spark.file.json

import org.apache.spark.sql.{Encoders, SparkSession}

object JsonReaderWithSchema extends App {

  val spark = SparkSession.builder().
    appName("JsonReaderWithSchema").
    master("local").
    getOrCreate()

  case class UserRating(rating_text: String,
                        rating_color: String,
                        votes: String,
                        aggregate_rating: String)

  case class RestaurantSchema(has_online_delivery: Int,
                              url: String,
                              user_rating: UserRating,
                              name: String,
                              cuisines: String,
                              is_delivering_now: Int,
                              deeplink: String,
                              menu_url: String,
                              average_cost_for_two: Int
                             )

  val restaurantSchema = Encoders.product[RestaurantSchema].schema

  val restaurantData = spark.read.schema(restaurantSchema).
    json("src/main/resources/restaurant_ex.json")

  restaurantData.show()

  restaurantData.printSchema()

  spark.close()
}
