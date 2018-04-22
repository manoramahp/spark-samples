package com.example.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONtoRDD {

    /*
    *
    1. Create a SparkSession.

        SparkSession spark = SparkSession
            .builder()
            .appName("Spark Example - Write Dataset to JSON File")
            .master("local[2]")
            .getOrCreate();

    2. Get DataFrameReader of the SparkSession.
        spark.read()
    3. Use DataFrameReader.json(String jsonFilePath) to read the contents of JSON to Dataset<Row>.
        spark.read().json(jsonPath)
    4. Use Dataset<Row>.toJavaRDD() to convert Dataset<Row> to JavaRDD<Row>.
        spark.read().json(jsonPath).toJavaRDD()

    * */

    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read JSON to RDD")
                .master("local[2]")
                .getOrCreate();

        // read list to RDD
        String jsonPath = "data/employees.json";
        JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();

        items.foreach(item -> {
            System.out.println(item);
        });
    }
}
