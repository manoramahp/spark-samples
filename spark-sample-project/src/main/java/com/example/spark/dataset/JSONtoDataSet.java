package com.example.spark.dataset;
import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

// Spark Dataset is the latest API, after RDD and DataFrame, from Spark to work with data.
// In this will explain how to read JSON file to Spark Dataset with an example.

// To read JSON file to Dataset in Spark

//   Create a Bean Class (a simple class with properties that represents an object in the JSON file).
//   Create a SparkSession.
//   Initialize an Encoder with the Java Bean Class that you already created. This helps to define the schema of JSON data we shall load in a moment.
//   Using SparkSession, read JSON file with schema defined by Encoder. SparkSession.read().json(jsonPath).as(beanEncoder); shall return a Dataset with records of Java bean type.

public class JSONtoDataSet {

    public static class Employee implements Serializable{
        public String name;
        public int salary;
    }

    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Read JSON File to DataSet")
                .master("local[2]")
                .getOrCreate();

        // Java Bean (data class) used to apply schema to JSON data
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);

        String jsonPath = "data/employees.json";

        // read JSON file to Dataset
        Dataset<Employee> ds = spark.read().json(jsonPath).as(employeeEncoder);
        ds.show();
    }
}
