package com.rainsoft.hbase.oracle;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 测试HBaseUtils工具类，的writeData2HBase方法
 * Created by CaoWeiDong on 2017-07-28.
 */
public class TestHbase {
    public static String[] fileds = {"col1", "col2", "col3", "col4"};
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("aaa")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Random random = new Random();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int[] aaa = new int[4];
            for (int j = 0; j < 4; j++) {
                aaa[j] = random.nextInt(900) + 100;
            }
            list.add(StringUtils.join(aaa, ","));
        }
        JavaRDD<String> originalRDD = sc.parallelize(list);

        originalRDD.foreach(
                (VoidFunction<String>) s -> System.out.println(s)
        );
        /*JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = originalRDD.flatMapToPair(
                new PairFlatMapFunction<String, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(String s) throws Exception {
                        String[] aa = s.split(",");

                    }
                }
        )*/
        sc.close();
    }
}
