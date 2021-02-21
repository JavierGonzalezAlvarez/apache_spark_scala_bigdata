package org.bigdata.application

// import apache.spark
import org.apache.spark._

object CountWords {

  // create a function
  def main(args: Array[String]) {

    // Assign  a SparkContext in the local machine
    val sc = new SparkContext("local[*]", "CountWords")

    // Put lines into RDD
    val input = sc.textFile("data/cervantes.txt")

    // Split words by a space
    val words_in_book = input.flatMap(x => x.split(" "))

    // Counting words and numbers
    val wordCounts = words_in_book.countByValue()
    val sortedResults = wordCounts.toSeq.sortBy(_._1)

    // Print results and sorted
    sortedResults.foreach(println)
  }

}


