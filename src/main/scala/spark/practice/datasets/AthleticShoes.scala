package spark.practice.datasets

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.DataFrame
import spark.utils.ContextBuilder

import scala.language.implicitConversions

object DFColumn extends DFColumn {
  implicit def columnToString(col: DFColumn.Value): String = col.toString
}

trait DFColumn extends Enumeration {
  val item_name,
  item_category,
  item_after_discount,
  item_price,
  item_rating,
  buyer_gender,
  item_shipping,
  percentage_solds = Value
}

object AthleticShoes extends App with ContextBuilder {

  override val appName: String = this.getClass.getSimpleName

  val fileLocation = "src/main/resources/athletic_shoes.csv"

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  import spark.implicits._

  val athleticShoesDF = spark.read.
    option("header", "true").
    option("inferSchema", "true").
    csv(fileLocation)


  def dropRowByItemNameCategoryIfNull(itemNameCol: String, itemCategoryCol: String)(df: DataFrame): DataFrame = {
    df.na.drop(Array(itemNameCol, itemCategoryCol))
  }

  def updateItemAfterDiscount(itemAfterDiscountCol: String, itemPriceCol: String)(df: DataFrame): DataFrame = {
    val condition = col(itemAfterDiscountCol).isNull

    df.
      withColumn(itemAfterDiscountCol,
        when(condition, col(itemPriceCol)).
          otherwise(col(itemAfterDiscountCol)))
  }

  def updateItemRating(itemRatingCol: String)(df: DataFrame): DataFrame = {
    val condition = col(itemRatingCol).isNull

    df.
      withColumn(itemRatingCol,
        when(condition, lit(0)).
          otherwise(col(itemRatingCol)))
  }

  def updateBuyerGender(buyerGenderCol: String)(df: DataFrame): DataFrame = {
    df.
      na.
      fill("unknown", List(buyerGenderCol))
  }

  def updatePercentageSolds(percentageSoldsCol: String)(df: DataFrame): DataFrame = {
    val condition = col(percentageSoldsCol).isNull

    df.
      withColumn(percentageSoldsCol,
        when(condition, lit(-1)).
        otherwise(col(percentageSoldsCol)))
  }

  def updateOtherCols(itemShippingCol: String, itemPriceCol: String)(df: DataFrame): DataFrame = {
    df.
      na.
      fill("n/a", List(itemShippingCol, itemPriceCol))
  }

  def updateRows(itemNameCol: String,
                 itemCategoryCol: String,
                 itemAfterDiscountCol: String,
                 itemPriceCol: String,
                 itemRatingCol: String,
                 buyerGenderCol: String,
                 percentageSoldsCol: String,
                 itemShippingCol: String
                )(df: DataFrame): DataFrame = {
    df.
      transform(dropRowByItemNameCategoryIfNull(itemNameCol, itemCategoryCol)).
      transform(updateItemAfterDiscount(itemAfterDiscountCol, itemPriceCol)).
      transform(updateItemRating(itemRatingCol)).
      transform(updateBuyerGender(buyerGenderCol)).
      transform(updatePercentageSolds(percentageSoldsCol)).
      transform(updateOtherCols(itemShippingCol, itemPriceCol))
  }

  val athleticShoesDS = athleticShoesDF.
    transform(updateRows(
      DFColumn.item_name,
      DFColumn.item_category,
      DFColumn.item_after_discount,
      DFColumn.item_price,
      DFColumn.item_rating,
      DFColumn.buyer_gender,
      DFColumn.percentage_solds,
      DFColumn.item_shipping)).
    as[Shoes]

  athleticShoesDS.show(91)

  spark.close()
}
