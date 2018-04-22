package com.example.spark.mllib;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

public class NaiveBayesClassifierExample {

    public static void main(String[] args) {

        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("JavaNaiveBayesExample")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // provide path to data transformed as [feature vectors]
        String path = "data/mllib/sample_libsvm_data.txt";
        JavaRDD inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();

        // split the data for training (60%) and testing (40%)
        JavaRDD[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
        JavaRDD training = tmp[0]; // training set
        JavaRDD test = tmp[1]; // test set

        // Train a Naive Bayes model
        NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

        // Predict for the test data using the model trained
        JavaPairRDD<Double, Double> predictionAndLabel =
                test.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        // calculate the accuracy
        double accuracy =
                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) test.count();

        System.out.println("Accuracy is : "+accuracy);

        // Save model to local for future use
        model.save(jsc.sc(), "target/tmp/myNaiveBayesModel");

        // stop the spark context
        jsc.stop();
    }
}
