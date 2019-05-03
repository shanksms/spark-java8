package com.sparkTutorial.pairRdd.aggregation.reducebykey;


import com.sparkTutorial.Utils;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AverageHousePriceProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */
        SparkConf conf = new SparkConf().setAppName("house-price").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> recordsWithoutHeader = sc.textFile("in/RealEstate.csv")
            .filter(line -> !line.split(Utils.COMMA_DELIMITER)[0].equalsIgnoreCase("MLS"));
        JavaPairRDD<Integer, AvgCount> bedroomAndPriceMap = recordsWithoutHeader.mapToPair(line -> new Tuple2<>(
            Integer.parseInt(line.split(Utils.COMMA_DELIMITER)[3]),
            new AvgCount(1, Double.parseDouble(line.split(Utils.COMMA_DELIMITER)[2]))
        ));

        Map<Integer, Double> result = bedroomAndPriceMap.reduceByKey((a1, a2) -> new AvgCount(a1.getCount() + a2.getCount(), a1.getTotal() + a2.getTotal()))
        .mapValues(avgCount -> avgCount.getTotal()/avgCount.getCount())
        .collectAsMap();

        for (Map.Entry<Integer, Double> entry : result.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

    }

}
