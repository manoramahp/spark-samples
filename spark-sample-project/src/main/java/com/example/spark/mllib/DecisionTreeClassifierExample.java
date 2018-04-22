package com.example.spark.mllib;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;

/**
 * Classification using Decision Trees in Apache Spark MLlib with Java Example
 */
public class DecisionTreeClassifierExample {

    public static void main(String[] args) {

        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("DecisionTreeExample")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // provide path to data transformed as [feature vectors]
        String path = "data/mllib/sample_libsvm_data.txt";
        JavaRDD inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();

        // split the data for training (60%) and testing (40%)
        JavaRDD[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
        JavaRDD trainingData = tmp[0]; // training set
        JavaRDD testData = tmp[1]; // test set

        // Set hyper parameters for Decision Tree algorithm
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        int numClasses = 2;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        int maxDepth = 5;
        int maxBins = 32;

        // Train a Decision Tree model
        DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);


        // Predict for the test data using the model trained
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        // calculate the accuracy
        double accuracy =
                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) testData.count();

        System.out.println("Accuracy is : "+accuracy);
        System.out.println("Trained Decision Tree model:\n" + model.toDebugString());

        // Save model to local for future use
        model.save(jsc.sc(), "myDecisionTreeClassificationModel");

        // stop the spark context
        jsc.stop();
    }

}
