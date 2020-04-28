package com.sparkTutorial.rdd.airports



import com.sparkTutorial.commons.Utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */
    Logger.getLogger("org").setLevel(Level.ERROR);
    System.setProperty("hadoop.home.dir", "C:\\Hadoop");
    val conf = new SparkConf().setAppName("Airport").setMaster("local[2]");
    val sc = new SparkContext(conf);

    val airports = sc.textFile("in/airports.text");
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"");

    val airportNameandCityName = airportsInUSA.map(l => {
      val splits = l.split(Utils.COMMA_DELIMITER)
      splits(1)+ ", "+splits(2)
    })

    airportNameandCityName.saveAsTextFile("out/airports_in_usa.text")

  }
}
