package com.sparkTutorial.rdd.nasaApacheWebLogs

import com.esotericsoftware.minlog.Log.Logger
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    System.setProperty("hadoop.home.dir", "C:\\Hadoop");
    val conf = new SparkConf().setAppName("ApacheLogWithSameHost").setMaster("local[3]");

    val sc = new SparkContext(conf);
    val julyLogs = sc.textFile("in/nasa_19950701.tsv");
    val augustLogs = sc.textFile("in/nasa_19950801.tsv");

    val julyHosts = julyLogs.map(line => {
     val splits =  line.split("\t")
      splits(0) + "\t" + splits(5)
    })
    val augHosts = augustLogs.map(line => {
      val splits = line.split("\t")
      splits(0) + "\t"+ splits(5);
    })

    val intersectionOfJulyAug = julyHosts.intersection(augHosts);
    val clearHeader = intersectionOfJulyAug.filter(host => host != "host");
    clearHeader.saveAsTextFile("out/nasa_logs_same_hosts.csv");
  }
}
