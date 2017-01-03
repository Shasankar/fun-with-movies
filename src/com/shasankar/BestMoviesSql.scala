//Find the best movies using SparkSql.
//Calculate average user rating for each movie.
//Consider movies with 5 or more ratings. 
package com.shasankar

import java.io.StringReader

import org.apache.spark.sql.SparkSession
import au.com.bytecode.opencsv.CSVReader

object BestMoviesSql {
    case class MovieDict(movieId: String, movieName: String)
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Best Movies SQL")
      .getOrCreate()
    import spark.implicits._
    // read ratings.csv containing userId,movieId,rating
    val movieRatingInput = spark.sparkContext.textFile(args(0))
    movieRatingInput.map { line =>
      val reader = new CSVReader(new StringReader(line))
      val tokens = reader.readNext()
      (tokens(1), tokens(2))
    }.filter(_._2.matches("\\d*\\.?\\d*"))
    .map(t => (t._1,t._2.toFloat))
    .toDF("movieId","rating").createOrReplaceTempView("movieRatings")
    // read movies.csv containing movieId,movieName,movieGenres
    val movieDictInput = spark.sparkContext.textFile(args(1))
    movieDictInput.map{line =>
      val reader = new CSVReader(new StringReader(line))
      val tokens = reader.readNext()
      MovieDict(tokens(0),tokens(1))
    }.filter(_.movieId.forall(_.isDigit))
    .toDF().createOrReplaceTempView("movieDict")
    spark.sql("set spark.sql.shuffle.partitions=20") //controls the no. of partions ONLY used by shuffle operations like join, by default its 200, this can also be acieved by .setconf on sqlcontext
    //val result = spark.sql("select movieId,rating from movieRatings")
    val result = spark.sql("select b.movieName, a.avgRating from " + 
        "(select movieId, avg(rating) as avgRating, count(rating) as countRating " + 
        "from movieRatings group by movieId) a " + 
        "join movieDict b on a.movieId = b.movieId " +
        "where a.countRating > 5 order by a.avgRating desc, b.movieName asc")
    // save output to csv
    result.write.csv(args(2))
  }
}