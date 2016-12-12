package org.dp.exercises.spark.functions;

import java.util.function.Function;

/**
 * Created by Y700-17 on 09.11.2016.
 */
public class JustExternalFunction implements org.apache.spark.api.java.function.Function<String, String> {

    @Override
    public String call(String value) throws Exception {
        return value+"XXX";
    }
}
