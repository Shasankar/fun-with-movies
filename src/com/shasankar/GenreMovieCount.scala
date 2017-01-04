//Total no. of movies in each genres
package com.shasankar

import org.apache.spark.sql.SparkSession

object GenreMovieCount {
  case class Movies(name: String, genres: String)
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Genre Movie Count")
      .getOrCreate()
    import spark.implicits._
    val moviesDS = spark.read.csv(args(0))
      .select($"_c1".alias("name"), $"_c2".alias("genres")).as[Movies]
    moviesDS.flatMap { movie =>
      var movieTuples = new collection.mutable.HashMap[String, Int]
      movie.genres.split("\\|").foreach { genre =>
        movieTuples += (genre -> 1)
      }
      movieTuples
    }.groupByKey(_._1)
      .count().coalesce(1)
      .write.csv(args(1))
  }
}