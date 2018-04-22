package com.example.spark.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Topic modelling using Latent Dirichlet Condition in Apache Spark MLlib
 */
public class TopicModellingLDAExample {
    public static void main(String[] args) {

        // Configure spark application
        SparkConf conf = new SparkConf().setAppName("TopicModellingLDAExampleApp")
                .setMaster("local[2]");
        // start Spark Context
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Load and parse the sample data
        String path = "data/mllib/sample_lda_data.txt";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.trim().split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });

        // Index documents with unique IDs : Long - document id, Vector - Transformed document contents
        JavaPairRDD<Long, Vector> corpus =
                JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
        corpus.cache();

        // Cluster the documents into three topics using LDA
        LDAModel ldaModel = new LDA().setK(3).run(corpus);

        // Output topics. Each is a distribution over words (matching word count vectors)
        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                + " words):");
        Matrix topics = ldaModel.topicsMatrix();
        for (int topic = 0; topic < 3; topic++) {
            System.out.print("Topic " + topic + ":");
            for (int word = 0; word < ldaModel.vocabSize(); word++) {
                System.out.print(" " + topics.apply(word, topic));
            }
            System.out.println();
        }

        // save the model
        ldaModel.save(jsc.sc(),
                "TopicModellingLDAExampleApp");

        // test if the model loads
        DistributedLDAModel sameModel = DistributedLDAModel.load(jsc.sc(),
                "TopicModellingLDAExampleApp");

        // stop the Spark Context
        jsc.stop();
    }
}
