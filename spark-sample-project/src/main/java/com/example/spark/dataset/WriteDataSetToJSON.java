package com.example.spark.dataset;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

// Dataset class provides an interface for saving the content of the non-streaming Dataset out into external storage.
// JSON is one of the many formats it provides.

//To write Spark Dataset to JSON file

//     Apply write method to the Dataset. Write method offers many data formats to be written to.
//     Dataset.write()
//     Use json and provide the path to the folder where JSON file has to be created with data from Dataset.
//     Dataset.write().json(pathToJSONout)

public class WriteDataSetToJSON {

    public static class Employee implements Serializable{
        public String name;
        public int salary;
    }

    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Write Dataset to JSON File")
                .master("local[2]")
                .getOrCreate();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String jsonPath = "data/employees.json";
        Dataset<Employee> ds = spark.read().json(jsonPath).as(employeeEncoder);

        // write dataset to JSON file
        ds.write().json("data/out_employees/");
    }
}
