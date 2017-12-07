package com.rainsoft.j2se;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Created by CaoWeiDong on 2017-10-20.
 */
public class TestSpark {
    public static void main(String[] args) {
        String[] arr = new String[]{"aa", "bb", "cc"};

        SparkSession spark = SparkSession.builder()
                .appName(TestSpark.class.getSimpleName())
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> dataRDD = jsc.parallelize(Arrays.asList(arr));
        JavaRDD<String> filterRDD = dataRDD.filter(
                (Function<String, Boolean>) v1 -> {
                    boolean ifFilter = true;
                    if ("bb".equalsIgnoreCase(v1)) {
                        ifFilter = false;
                    }
                    return ifFilter;
                }
        );
        filterRDD.foreach(
                (VoidFunction<String>) s -> System.out.println(s)
        );
    }
}
