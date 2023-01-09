package part5lowlevel

import org.apache.spark.sql.SparkSession

import scala.io.Source

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(company: String, date: String, price: Double)
  def readStocks(filename: String) = {
    Source.fromFile(filename)
      .getLines()
      .drop(1) // drop the header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // drop the header using filter
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  val stocksRDD4 = stocksDF.rdd // RDD of row (bc the stocksDF is a DataFram, so you lose types info)
  // if you want to keep type info, use Dataset

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type info, DataFrames only have rows

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // you get to keep type info

}
