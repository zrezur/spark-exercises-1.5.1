package org.dp.exercises.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by Y700-17 on 09.11.2016.
 */
public class MainForSparkLauncher {
    final static Logger logger = LoggerFactory.getLogger(MainForSparkLauncher.class);

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<Integer> collection = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1));
        long count = collection.count();
        System.out.println("XXX RESULT: "+count);
        logger.info("XXX dziala {}", count);

        long count1 = collection.distinct().count();
        System.out.println("XXX After distinct: "+count1);
        logger.info("XXX After distinct {}", count1);
    }
}
