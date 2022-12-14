package part5lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

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
  case class StockValue(symbol: String, date: String, price: Double)
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

  // Transformations

  // counting
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION

  // distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max -- based on the ordering
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price) // alternatives below also works
  //  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  //  implicit val stockOrdering = Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  //  implicit val stockOrdering = Ordering.fromLessThan((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive -- shuffling

  // Partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  /*
  Repartitioning is EXPENSIVE. Involves Shuffling.
  Best practice: partition EARLY, the process that.
  Size of a partition 10-100MB.
   */

  // coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   *
   * 1. Read the movies.json as an RDD.
   * 2. Show the distinct genres as an RDD.
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   * 4. Show the average rating of movies by genre.
   */

  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
  .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(
      col("Title").as("title")
      ,col("Major_Genre").as("genre")
      ,col("IMDB_Rating").as("rating")
    ).where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

//  moviesRDD.toDF.show()
//  genresRDD.toDF.show()
//  goodDramasRDD.toDF.show()

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show
}
