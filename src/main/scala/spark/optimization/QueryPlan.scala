package spark.optimization

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{avg, col, lit}
import spark.utils.ContextBuilder

object QueryPlan extends App with ContextBuilder{
  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

//  val columns = Seq("user", "points")
//  val data = Seq(("A", "10"), ("B", "20"), ("C", "30"))
//
//  import spark.implicits._
//
//  val usersDF = data.toDF(columns: _*)
//
//  val pointsDF = usersDF.agg(functions.sum("points").as("total"))
//  pointsDF.explain()
//
//  val DfA = spark.range(1, 100000000).repartition(20)
//  val DfB = spark.range(1, 500000000, 5)
//
//  val joinedDF = DfA.join(DfB, "id")
//  joinedDF.take(1)
//
//  joinedDF.explain()

  /*
  salary, department
  120000, Finance
  130000, Finance,
  100000, Finance,
  70000, Sales,
  65000, Sales
   */
//  val employeeDF = spark.read.
//    option("header", "true").
//    csv("src/main/resources/employee_test.csv")
//
//  val depAvgSalaryDF: DataFrame = employeeDF.
//    groupBy(col("department")).
//    agg(avg(col("salary")))
//
//  depAvgSalaryDF.explain()

  def addColumn(df: DataFrame, n: Int) = {
    val columns = (1 to n).map(col => s"col_$col")
    columns.foldLeft(df)((df, column) => df.withColumn(column, lit("n")))
  }

  val data1 = (1 to 500000).map(i => (i, i * 100))
  val data2 = (1 to 10000).map(i => (i, i * 1000))

  import spark.implicits._

  val df1 = data1.toDF("id", "salary").repartition(5)
  val df2 = data2.toDF("id", "salary").repartition(10)

//  val dfWithColumns = addColumn(df2, 10)
//  val joinedDF1 = dfWithColumns.join(df1, "id")
//
//  joinedDF1.explain()

  val repartitionedById1 = df1.repartition(col("id"))
  val repartitionedById2 = df2.repartition(col("id"))

  val dfWithColumns1 = addColumn(repartitionedById1, 10)

  val joinedDF2 = repartitionedById2.join(dfWithColumns1, "id")

  joinedDF2.explain()

  spark.close()
}
