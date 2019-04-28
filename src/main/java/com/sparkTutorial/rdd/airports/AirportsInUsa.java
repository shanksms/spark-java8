package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsa {

  public static void main(String[] args) {
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
    SparkConf sparkConf = new SparkConf().setAppName("airport-in-usa").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> airports = sc.textFile("in/airports.text");
    airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equalsIgnoreCase("\"United States\""))
        .map(line -> {
          String [] tokens = line.split(Utils.COMMA_DELIMITER);
          return String.join(",", tokens[1], tokens[2]);
        }).saveAsTextFile("out/airports_in_usa.text");
  }

}
