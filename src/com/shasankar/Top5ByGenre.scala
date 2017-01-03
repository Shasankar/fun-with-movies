//Find top 5 movies based on avg user rating in each genre

package com.shasankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Top5ByGenre {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Top 5 Movies By Genre")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    // read movies.csv containing movieId,movieName,movieGenres
    val moviesDf = spark.read.csv(args(0)).cache
    moviesDf.select($"_c0", $"_c1", explode(split($"_c2", "\\|"))).createOrReplaceTempView("MovieDetails")
    // read ratings.csv containing userId,movieId,rating
    val ratingsDf = spark.read.csv(args(1))
    ratingsDf.createOrReplaceTempView("MovieRatings")
    val result = spark.sql("select name, rating, genre from " +
      "(select b._c1 as name, a.avgRat as rating, b.col as genre, dense_rank() over (partition by b.col order by a.avgRat desc) as rank from " +
      "(select _c1 as movieId, avg(_c2) as avgRat, count(_c2) as countRat from MovieRatings group by _c1) a join " +
      "MovieDetails b " +
      "on a.movieId = b._c0 where a.countRat > 5) where rank<=5")
    // save output to csv file  
    result.coalesce(1).write.csv(args(2))
  }
}