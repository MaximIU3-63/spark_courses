package spark.scd2.utils

private[scd2] object Messages {
  lazy val requireMessage: String => String = (value: String) => {
    s"""
       | Val $value shouldn't be empty.
       |""".stripMargin
  }
}
