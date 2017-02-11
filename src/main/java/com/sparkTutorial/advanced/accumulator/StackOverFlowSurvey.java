package com.sparkTutorial.advanced.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

public class StackOverFlowSurvey {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");

        SparkContext sparkContext = new SparkContext(conf);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();

        total.register(sparkContext, Option.apply("total"), false);
        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle point"), false);

        JavaRDD<String> responseRDD = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv");

        JavaRDD<String> responseFromCanada = responseRDD.filter(response -> {
            String[] splits = response.split(",", -1);

            total.add(1);

            if (splits[14].equals("")) {
                missingSalaryMidPoint.add(1);
            }

            return splits[2].equals("Canada");

        });

        System.out.println("Count of responses from Canada: " + responseFromCanada.count());
        System.out.println("Total count of responses: " + total.value());
        System.out.println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());
    }
}
