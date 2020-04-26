package com.sparkTutorial.rdd.sumOfPrimeNums;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SumOfFirst100PrimeNumbersSolution {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");

        JavaRDD<String> numbers = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        JavaRDD<String> validNumbers = numbers.filter(number -> !number.equals(""));

        String sum = validNumbers.reduce((x, y) -> Integer.valueOf(x) + Integer.valueOf(y) + "");

        System.out.println("Sum is: " + sum);
    }
}
