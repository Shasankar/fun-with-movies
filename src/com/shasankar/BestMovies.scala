//Find the best movies.
//Calculate average user rating for each movie.
//Consider movies with 5 or more ratings. 

package com.shasankar

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader


object BestMovies {
  def main(args: Array[String]){
    val sc = new SparkContext("local[*]","BestMovies")
    // read movies.csv containing movieId,movieName,movieGenres
    val movies = Source.fromFile(args(0),"UTF-8").getLines() 
    val moviesMap = new collection.mutable.HashMap[String,String]
    for(line <- movies) {
      val reader = new CSVReader(new StringReader(line))
      val tokens = reader.readNext
      moviesMap += (tokens(0)->tokens(1))
    }
    val mvMap = sc.broadcast(moviesMap)
    //read ratings.csv containing userId,movieId,rating
    val input = sc.textFile(args(1),4) // minNumPartitions=4 by default results in 2 partitions
    input.filter(_(0).isDigit)
    .map(_.split(","))
    .map(l => (l(1),l(2).toFloat))
    .combineByKey(
        rating => (rating,1),
        (acc: (Float,Int), rating) => (acc._1+rating,acc._2+1),
        (acc1: (Float,Int), acc2: (Float,Int)) => (acc1._1+acc2._1, acc1._2+acc2._2))
    .filter(_._2._2>=5)
    .map{ case(movieId,ratingSum) => (movieId,ratingSum._1/ratingSum._2.toFloat)}
    .sortBy(r => (r._2,r._1),false)
    .map(r => (mvMap.value(r._1),r._2))
    .coalesce(1) // For output to a single file
    //save results to output dir
    .saveAsTextFile(args(2))
  }
}