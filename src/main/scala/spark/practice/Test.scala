package spark.practice

import org.apache.spark.sql.functions.{col, split}
import spark.utils.ContextBuilder

object Test extends App with ContextBuilder{
  override val appName: String = this.getClass.getSimpleName

  import spark.implicits._

  val df = Seq(
    (1, "qw,45,64,dsfg,dsd")
  ).toDF("id", "concat")

  val df2 = Seq(
    (1, "wer")
  ).toDF("id", "nm")

  df.join(df2, Seq("id"), "inner").show

  spark.close()
}
