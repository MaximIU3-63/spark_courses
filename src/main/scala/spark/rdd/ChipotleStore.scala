package spark.rdd

import spark.utils.ContextBuilder

import scala.io.Source

object ChipotleStore extends App with ContextBuilder{
  override val appName: String = this.getClass.getSimpleName

  val sc = spark.sparkContext

  case class Store(
                    state: String,
                    location: String,
                    address: String,
                    latitude: Double,
                    longitude: Double
                  )

  //1. обычное считывание данных из файла, которое возможно сделать средствами import scala.io.Source
  def readStores(fileName: String): List[Store] = {
    val srcFiles = Source.fromFile(fileName)
    srcFiles.getLines().
      drop(1).
      map(line => line.split(",")).
      map(values => {
        Store(
          values(0),
          values(1),
          values(2),
          values(3).toDouble,
          values(4).toDouble
        )
      }).toList
  }
  val storesRDD = sc.parallelize(readStores("src/main/resources/chipotle_stores.csv"))
  storesRDD.foreach(println)

  //2. использование метода  .textFile
  val storesRDD2 = sc.textFile("src/main/resources/chipotle_stores.csv")
    .map(line => line.split(","))
    .filter(values => values(0) == "Alabama")
    .map(values => Store(
      values(0),
      values(1),
      values(2),
      values(3).toDouble,
      values(4).toDouble))

  storesRDD2.foreach(println)

  //3.1 Напрямую из DF в RDD[Row]
  val storesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/chipotle_stores.csv")


  val storesRDD3 = storesDF.rdd

  storesRDD3.foreach(println)

  //3.2 DF -> DS ->  RDD[Store]

  import spark.implicits._

  val storesDS = storesDF.as[Store]
  val storesRDD4 = storesDS.rdd

  storesRDD4.foreach(println)
}
