package com.sparkTutorial.rdd.sumOfNumbers;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        SparkConf sparkConf = new SparkConf().setAppName("sum").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");
        int sum = lines.flatMap( line -> Arrays.asList(line.split("\\s+")).iterator())
            .filter(numStr -> !numStr.isEmpty())
            //.map(numStr -> numStr.trim())
            .map(numStr -> Integer.parseInt(numStr)).reduce( (num1, num2) -> num1 + num2);
        System.out.println(sum);
    }
}
