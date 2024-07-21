package transformer_spark_scala.extract.columns

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object KantorColumns {
  val rraCols: Seq[Column] = Seq(col("rra_pk"),
    col("client_id").as("client_pk")
  )
}
