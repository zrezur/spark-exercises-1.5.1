package org.dp.exercises.spark;


import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * Created by Y700-17 on 09.11.2016.
 */
public class SubmitSparkLauncher {
    public static void main(String[] args) throws IOException, InterruptedException {
        final Process process = new SparkLauncher()
                .setMainClass(MainForSparkLauncher.class.getName())
                .setMaster("spark://192.168.0.100:7077")
                .setSparkHome("d:\\spark\\master")
                .setAppResource("C:\\dev\\spark-exercises\\target\\spark-exercises-1.0-SNAPSHOT.jar")
                .setVerbose(true)
                .setDeployMode("cluster")
                .launch();

        new Thread(() -> {
            try {
                IOUtils.copy(process.getInputStream(), System.out);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
        new Thread(() -> {
            try {
                IOUtils.copy(process.getErrorStream(), System.err);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
//        new Thread(new Runnable() {
//            public void run() {
//                IOUtils.copy(process.getOutputStream(), System.out);
//            }
//        });
//        process.
        process.waitFor();

    }
}
