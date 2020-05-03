package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.hadoop.hdfs.DFSClient.Conf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountryProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ....
     */
    Logger.getLogger("org").setLevel(Level.OFF);
    val conf = new SparkConf().setAppName("GroupBy").setMaster("local[2]");
    val sc = new SparkContext(conf);

    val airpordRDD = sc.textFile("in/airports.text");
    val airportPairRDD = airpordRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(3) ,
                                                  line.split(Utils.COMMA_DELIMITER)(1)));

    val airportByCountryName = airportPairRDD.groupByKey();
    for((country, airport) <- airportByCountryName.collectAsMap())
      println(country + " : " + airport);
  }
}
