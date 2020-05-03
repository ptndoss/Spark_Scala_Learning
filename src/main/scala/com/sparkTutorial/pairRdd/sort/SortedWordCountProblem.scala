package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

  def main(arrays : Array[String]): Unit ={
    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    Logger.getLogger("org").setLevel(Level.OFF);
  val conf = new SparkConf().setAppName("OrderBy").setMaster("local[1]");
  val sc = new SparkContext(conf);

  val wordCountRDD  = sc.textFile("in/word_count.text");
//  val cleanWordsRDD = wordCountRDD.filter(line => line.trim != "");
  val eachWordRDD = wordCountRDD.flatMap(line => line.split(" "));

  val wordPairRDD = eachWordRDD.map(line => (line,1));
  val wordCountPairRDD = wordPairRDD.reduceByKey((x,y) => (x+y))

  val countToWordPairRDD = wordCountPairRDD.map(line => (line._2, line._1));

  val sortedWordCount = countToWordPairRDD.sortByKey(ascending = false);

  val finalSortedWordCountRDD = sortedWordCount.map(line => (line._1, line._2));

  for((words, count) <- finalSortedWordCountRDD.collect())
    println(words + " : " + count);

  }
}

