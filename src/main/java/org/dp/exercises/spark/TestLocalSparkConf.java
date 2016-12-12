package org.dp.exercises.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Y700-17 on 29.11.2016.
 */
public class TestLocalSparkConf {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("localTest");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sc.textFile("d:\\spark\\cities.csv", 1);
        System.out.println(stringJavaRDD.count());
    }
}
