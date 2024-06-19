package spark.rdd

import spark.utils.ContextBuilder

object RDDExamples extends App with ContextBuilder{
  override val appName: String = this.getClass.getSimpleName

  spark.sparkContext.setLogLevel("WARN")

  //1. Создадим список id значений List(id-0, id-1, id-2, id-3, id-4)
  val ids = List.fill(5)("id-").zipWithIndex.map(x => x._1 + x._2)

  import spark.implicits._

  val idsDS = ids.toDF.as[String]

  val idsPartitioned = idsDS.repartition(6)

  val numPartitions = idsPartitioned.rdd.partitions.length
  println(s"partitions = ${numPartitions}")

  idsPartitioned.rdd.mapPartitionsWithIndex(
    (partition: Int, it: Iterator[String]) => it.map(id => {
      println(s"partition -> ${partition}; id -> ${id}")
    })
  ).collect()

  spark.close()
}
