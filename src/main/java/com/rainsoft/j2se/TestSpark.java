package com.rainsoft.j2se;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
        System.out.println("lksdjflkasjdlf");
        SparkConf conf = new SparkConf()
                .setAppName(TestSpark.class.getSimpleName())
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = sc.parallelize(list);
        JavaRDD<String> filterRDD = dataRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        boolean ifFilter = true;
                        if ("bb".equalsIgnoreCase(v1)) {
                            ifFilter = false;
                        }
                        return ifFilter;
                    }
                }
        );
        filterRDD.foreach(
                new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                }
        );
    }
}
