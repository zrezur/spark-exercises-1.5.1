package org.dp.exercises.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dp.exercises.spark.functions.JustExternalFunction;

public class SubmitFirstJob {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test1")
                .setMaster("local")
                .setSparkHome("d:\\spark\\master ")
                .setJars(new String[]{"C:\\dev\\spark-exercises\\target\\spark-exercises-1.0-SNAPSHOT.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> stringJavaRDD = sc.textFile("d:\\spark\\cities.csv", 1);

        JavaRDD<String> rdd = stringJavaRDD.map(new JustExternalFunction());

        try{
            System.out.println(rdd.count());
        }
        catch(Exception e){
            throw new RuntimeException(e);
//            System.out.println("XXXXXXXXXXXXXXX 2nd time");
//            System.out.println(rdd.count());
        }

    }

}
