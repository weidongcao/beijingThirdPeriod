package com.rainsoft.j2se;

import com.rainsoft.FieldConstants;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by CaoWeiDong on 2017-10-20.
 */
public class TestSpark {
    public static void main(String[] args) {

        String[] arr = FieldConstants.BCP_FILE_COLUMN_MAP.get("bcp_ftp");
        List<Integer[]> list = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < 9; i++) {
            Integer[] ints = new Integer[31];
            for (int j = 0; j < 31; j++) {
                ints[j] = rand.nextInt();
            }
            list.add(ints);
        }
        SparkSession spark = SparkSession.builder()
                .appName(TestSpark.class.getSimpleName())
                .master("local")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Integer[]> dataRDD = jsc.parallelize(list);
        JavaPairRDD<String, Integer> pairRDD = dataRDD.flatMapToPair(
                (PairFlatMapFunction<Integer[], String, Integer>) ints -> {
                    List<Tuple2<String, Integer>> list1 = new ArrayList<>();
                    for (int i = 0; i < ints.length; i++) {
                        String key = arr[i];
                        Integer value = ints[i];
                        list1.add(new Tuple2<>(key, value));
                    }
                    return list1.iterator();
                }
        );
        pairRDD.foreach(
                (VoidFunction<Tuple2<String, Integer>>) tuple -> System.out.println(tuple.toString())
        );
        jsc.close();
    }
}
