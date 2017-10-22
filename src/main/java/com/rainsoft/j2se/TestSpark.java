package com.rainsoft.j2se;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by CaoWeiDong on 2017-10-20.
 */
public class TestSpark {
    public static void main(String[] args) {
        String[] arr = new String[]{"aa", "bb", "cc"};

        List<String> list = Arrays.asList(arr);
        SparkConf conf = new SparkConf()
                .setAppName(TestSpark.class.getSimpleName())
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = sc.parallelize(list);
        dataRDD.foreach(
                new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                }
        );
    }
}
