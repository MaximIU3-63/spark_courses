import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column

trait Columns {
  val cols: Seq[Column]
}

object Rra extends Columns {
  override val cols: Seq[Column] = Seq(
    col("rra_pk"),
    col("ra_id")
  )
}

object Playground extends App {

  def extract(tableName: String, cols: Seq[Column]): Unit = {
    println(
      s"""
         |select
         | ${cols.mkString(", \n ")}
         |from $tableName
         |""".stripMargin)
  }

  extract("rra", Rra.cols)
}