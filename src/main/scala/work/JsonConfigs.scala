package work

case class JsonConfigs(
                        database: String,
                        table: String,
                        join_cols: List[String],
                        sensitive_cols: List[String],
                        insensitive_cols: List[String],
                        effective_from_col_name: Option[String],
                        effective_to_col_name: Option[String],
                        date_format: Option[String],
                      )