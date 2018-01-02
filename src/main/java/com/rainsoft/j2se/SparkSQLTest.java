package com.rainsoft.j2se;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-04-26.
 */
public class SparkSQLTest {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(SparkSQLTest.class.getSimpleName())
                .setMaster("local[4]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, String> options = new HashMap<>();
        options.put("url", ConfigurationManager.getProperty("oracle.url"));
        options.put("dbtable", "service_info");


        sc.close();
    }
}
