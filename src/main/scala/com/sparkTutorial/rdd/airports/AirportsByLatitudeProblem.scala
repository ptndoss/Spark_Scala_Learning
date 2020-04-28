package com.sparkTutorial.rdd.airports

import java.util.logging.{Level, Logger}

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}
import sun.misc.ObjectInputFilter.Config

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */
    Logger.getLogger("AirportWithLatitude").setLevel(Level.ALL);
    System.setProperty("hadoop.home.dir", "C:\\Hadoop");
    val conf = new SparkConf().setAppName("AirportWithLatitude").setMaster("local[2]");
    val sc = new SparkContext(conf);


    val airports = sc.textFile("in/airports.text");
    val airporstWithLatitude = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40);

    val airportNameAndLat = airporstWithLatitude.map(
      line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        splits(1) + ", " + splits(6)
        })

    airportNameAndLat.saveAsTextFile("out/airports_by_latitude.text");
  }
}
