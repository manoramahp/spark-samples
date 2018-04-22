package com.example.spark.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

//To add a new column to Dataset in Apache Spark
//
//    Use withColumn() method of the Dataset.
//    Provide a string as first argument to withColumn() which represents the column name.
//    Useorg.apache.spark.sql.functions  class for generating a new Column, to be provided as second argument.
//    Spark functions class provides methods for many of the mathematical functions like statistical, trigonometrical, etc.


public class DatasetAddColumn {

    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Add a new Column to Dataset")
                .master("local[2]")
                .getOrCreate();

        String jsonPath = "data/employees.json";
        Dataset<Row> ds = spark.read().json(jsonPath);

        // dataset before adding enw column
        ds.show();

        // add column to ds
        Dataset<Row> newDs = ds.withColumn("new_col",functions.lit(1));

        // print dataset after adding new column
        newDs.show();

        spark.stop();
    }
}