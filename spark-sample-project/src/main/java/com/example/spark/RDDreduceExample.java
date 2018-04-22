package com.example.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// reduce an RDD to a single element.
// Reduce is an aggregation of elements using a function.

/*
Following are the two important properties that an aggregation function should have

Commutative
    A+B = B+A  – ensuring that the result would be independent of the order of elements in the RDD being aggregated.
Associative
    (A+B)+C = A+(B+C) – ensuring that any two elements associated in the aggregation at a time does not effect the final result.
*/
public class RDDreduceExample {

    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // read text file to RDD
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(14,21,88,99,455));

        // aggregate numbers using addition operator
        int sum = numbers.reduce((a,b)->a+b);

        System.out.println("Sum of numbers is : "+sum);
    }

}
