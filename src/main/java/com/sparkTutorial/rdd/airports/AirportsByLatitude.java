package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitude {

  public static void main(String[] args) {
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
    SparkConf sparkConf = new SparkConf().setAppName("airport-latitude").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> airports = sc.textFile("in/airports.text");
    airports.filter(line -> Double.parseDouble(line.split(Utils.COMMA_DELIMITER)[6]) > 40)
    .map(
        line -> {
          String [] tokens = line.split(Utils.COMMA_DELIMITER);
          return String.join(",", tokens[1], tokens[6]);
        }
    ).saveAsTextFile("out/airports_by_latitude.text");

  }

}
