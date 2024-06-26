package work

import org.apache.spark.sql.functions.{col, collect_list, concat_ws, split}
import spark.utils.ContextBuilder

object ArrayToColTest extends App with ContextBuilder{
  override val appName: String = "test"

  import spark.implicits._

  // Creating List of Iterables
  val df = Seq(
    (1765, 1, "maksim", 1998, 3, 10, "2024-01-01"),
    (1123, 1, "ruyt", 1994, 7, 15, "2024-04-01")
  ).toDF("mdm_id", "id", "name", "year", "month", "day", "date").
    withColumn("concat_cols", split(concat_ws(",", col("id"), col("name"), col("year"), col("month"), col("day")), ",")).
    select(
      col("mdm_id"),
      col("concat_cols"),
      col("date")
    )

  df.printSchema()

  df.show(false)

  val unzipDF = df.
    select(
      df("mdm_id") +: (0 until 5).map(i => df("concat_cols").getItem(i).alias(s"concat_cols$i")): _*
    )

  unzipDF.show()

  spark.close()
}
