package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {

  public static void main(String[] args) {
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

    SparkConf sparkConf = new SparkConf().setAppName("same-host").setMaster("local[1]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> julData = sc.textFile("in/nasa_19950701.tsv").filter(line -> !line.startsWith("host"));
    JavaRDD<String> augData = sc.textFile("in/nasa_19950801.tsv").filter(line -> !line.startsWith("host"));

    JavaRDD<String> resultRDD = julData.map(line -> line.split("\\s+")[0])
        .intersection(
            augData.map(line -> line.split("\\s+")[0])
        );
    resultRDD.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    sc.close();

  }

}
