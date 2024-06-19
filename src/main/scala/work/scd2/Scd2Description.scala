package work.scd2

import org.apache.spark.sql.Column

trait Scd2Description {
  val insensitiveCols: Column
  val sensitiveCols: Column
}
