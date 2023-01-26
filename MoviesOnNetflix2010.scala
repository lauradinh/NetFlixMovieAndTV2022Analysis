package Exercises

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MoviesOnNetflix2010 {

  case class Netflix(index: Int, id: String, title: String, type_media: String, year: Int, rating: String, runtime: Int)

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("NetflixMovies2010")
      .master("local[*]")
      .getOrCreate()

    // infer schema using header
    import spark.implicits._
    val netflix = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/netflixmoviedata/raw_titles.csv")
      .as[Netflix]

//    netflix.printSchema();

    // filter for 2010
    val netflix2010 = netflix.filter($"year" === 2010)
    // filter out shows
    val netflixMovies2010 = netflix.filter($"type_media" === "MOVIE")
    // show the titles and runtimes of movies
    val netflixMovies2010Names = netflix.select("title", "runtime").sort($"runtime".desc).show()
  }

}
