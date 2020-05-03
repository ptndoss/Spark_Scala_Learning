package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("SumOfPrimeNos").setMaster("local[1]");
    val sc = new SparkContext(conf);

    val primeNumbersRDD = sc.textFile("in/prime_nums.text");
//    val primeNumbersRDD = primeNumbers.
    val primeRDD = primeNumbersRDD.flatMap(line => line.split("\\s+"));
//    println(primeRDD);
    val validNumbers = primeRDD.filter(number => !number.isEmpty);
    val intNumbers = validNumbers.map(number => number.toInt);
    val sumOfPrimeNumers = intNumbers.reduce((x,y) => x + y);
    println(sumOfPrimeNumers);
  }
}
