package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select($"Title", lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = $"Major_Genre" equalTo "Drama"
  val goodRatingFilter = $"IMDB_Rating" > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select($"Title").where(dramaFilter)
  // + multiple ways of filtering

  val moviesWithGoodnessFlags = moviesDF.select($"Title", preferredFilter.as("good_movie"))
  // filter on a boolean column
  moviesWithGoodnessFlags.where("good_movie") // where(col("good_movie") === "true")

  // negations
  moviesWithGoodnessFlags.where(not($"good_movie"))

  // Numbers

  // math operators
  val moviesAvgRatingsDF = moviesDF.select($"Title", ($"Rotten_Tomatoes_Rating" / 10 + $"IMDB_Rating") / 2)

  // correlation = pearson correlation: number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")/* corr is an ACTION */)

  // Strings

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*").where($"Name".contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    $"Name",
    regexp_extract($"Name", regexString, 0).as("regex_extract")
  ).where($"regex_extract" =!= "").drop("regex_extract")

  vwDF.select(
    $"Name",
    regexp_replace($"Name", regexString, "People's Car").as("regex_replace")
  )

/**
 * Exercise
 * Filter the cars DF by a list of car names obtained by an API call
 * Versions:
 *  - contains
 *  - regexes
 */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volkswagen|mercedez-benz|ford

  carsDF.select(
    $"Name",
    regexp_extract($"Name", complexRegex, 0).as("regex_extract")
  ).where($"regex_extract" =!= "")
    .drop("regex_extract")
//    .show()

  //version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()

}
