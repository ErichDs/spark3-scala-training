package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count($"Major_Genre")) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls

  // counting ditinct
  moviesDF.select(countDistinct($"Major_Genre")).show()

  // approximate count
  moviesDF.select(approx_count_distinct($"Major_Genre")).show()

  // min and max
  val minRatingDF = moviesDF.select(min($"IMDB_Rating"))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum($"US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg($"Rotten_Tomatoes_Rating"))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean($"Rotten_Tomatoes_Rating"),
    stddev($"Rotten_Tomatoes_Rating")
  ).show()

  // Grouping

  val countByGenreDF = moviesDF.groupBy($"Major_Genre") // includes null
    .count() // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDF = moviesDF
    .groupBy($"Major_Genre")
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy($"Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy($"Avg_Rating")

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US Gross revenue PER DIRECTOR
   */

  // 1.
  moviesDF.select(
    sum($"US_Gross" + $"Worldwide_Gross" /*+ $"US_DVD_Sales"*/)
  ).show()

  // 2.
  moviesDF.select(
    countDistinct($"Director")
  )
    .show()

  // 3.
  moviesDF
    .select(
      avg("US_Gross").as("Avg_US_Gross"),
      stddev("US_Gross").as("StdDev_US_Gross")
    )
    .show()

  // 4.
  moviesDF.groupBy($"Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross")
    )
    .orderBy("Avg_IMDB_Rating")
    .show()
}
