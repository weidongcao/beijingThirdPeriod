package com.rainsoft.spark;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 伴随分析
 * 指定设备范围
 * 分析哪两个Mac是同时出现的。
 * Created by CaoWeiDong on 2017-12-14.
 */
public class AccompanyPairAnalysis implements Serializable{
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName(AccompanyPairAnalysis.class.getSimpleName())
                .master("local[4]")
                .getOrCreate();
        /*StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("community", DataTypes.StringType, true),
                DataTypes.createStructField("mac", DataTypes.StringType, true)
        ));*/


        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> dataRDD = jsc.textFile("file:///E:\\data\\track-1w.txt")
                .map(
                        (Function<String, Row>) s -> RowFactory.create((Object[]) s.split(","))
                );

        //设备id与mac的Key-Value
        JavaPairRDD<String, String> equipmentMacPairRAD = dataRDD.mapPartitionsToPair(
                (PairFlatMapFunction<Iterator<Row>, String, String>) iter -> {
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    while (iter.hasNext()) {
                        Row row = iter.next();
                        list.add(new Tuple2<>(row.getString(0), row.getString(1)));
                    }
                    return list.iterator();
                }
        );

        //获取同一设备下不重复的mac地址
        JavaPairRDD<String, List<String>> equipmentDistinctMacsRAD = equipmentMacPairRAD.aggregateByKey(
                new ArrayList<>(),
                (Function2<List<String>, String, List<String>>) (macs, mac) -> {
                    if (!macs.contains(mac)) {
                        macs.add(mac);
                    }
                    return macs;
                },
                (Function2<List<String>, List<String>, List<String>>) (macs1, macs2) -> {
                    for (String mac : macs2) {
                        if (!macs1.contains(mac)) {
                            macs1.add(mac);
                        }
                    }
                    return macs1;
                }
        );

        //同一设备下任意两个Machine地址进行配对（不重复）
        JavaPairRDD<String, String> twoMacsPairRDD = equipmentDistinctMacsRAD.flatMapToPair(
                (PairFlatMapFunction<Tuple2<String, List<String>>, String, String>) tuple -> {
                    List<String> macs = tuple._2;
                    List<Tuple2<String, String>> pairs = new ArrayList<>();
                    while (macs.size() > 0) {
                        String keyMac = macs.get(0);
                        macs.remove(0);

                        if (macs.size() > 0) {
                            for (String valueMac : macs) {
                                pairs.add(new Tuple2<>(tuple._1, String.join(",", keyMac, valueMac)));
                            }
                        }
                    }
                    return pairs.iterator();
                }
        );

        //key, Value反转
        JavaPairRDD<String, String> pairMacToEquipmentRAD = twoMacsPairRDD.mapPartitionsToPair(
                (PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, String>) iter -> {
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    while (iter.hasNext()) {
                        Tuple2<String, String> tuple = iter.next();
                        list.add(new Tuple2<>(tuple._2, tuple._1));
                    }
                    return list.iterator();
                }
        );

        //两个人在不同的设备下出现的个数
        JavaPairRDD<String, List<String>> pairMacAppearEquipmentsRAD = pairMacToEquipmentRAD.aggregateByKey(
                new ArrayList<>(),
                (Function2<List<String>, String, List<String>>) (equipments, equipment) -> {
                    equipments.add(equipment);
                    return equipments;
                },
                (Function2<List<String>, List<String>, List<String>>) (equipments1, equipments2) -> {
                    equipments1.addAll(equipments2);
                    return equipments1;
                }
        );

        //如果两个人只在一个设备下出现，过滤掉
        JavaPairRDD<String, List<String>> pairMacAppearManyEquipmentsRAD = pairMacAppearEquipmentsRAD.filter(
                (Function<Tuple2<String, List<String>>, Boolean>) tuple -> tuple._2.size() > 1
        );

        //再次翻转，以出现的设备列表为key，以两个人的mac地址为Value
        JavaPairRDD<List<String>, String> equipmentsToPairMacRAD = pairMacAppearManyEquipmentsRAD.mapPartitionsToPair(
                (PairFlatMapFunction<Iterator<Tuple2<String, List<String>>>, List<String>, String>) iter -> {
                    List<Tuple2<List<String>, String>> list = new ArrayList<>();
                    while (iter.hasNext()) {
                        Tuple2<String, List<String>> tuple = iter.next();
                        list.add(new Tuple2<>(tuple._2, tuple._1));
                    }
                    return list.iterator();
                }
        );


        //根据两个人同时出现的设备数量进行排序
        JavaPairRDD<List<String>, String> sortedPairMacAppearEquipmentsRAD = equipmentsToPairMacRAD.sortByKey(
                new MyComparator(),
                false,
                1
        );

        List<Tuple2<List<String>, String>> result = sortedPairMacAppearEquipmentsRAD.take(100);

        for (Tuple2<List<String>, String> tuple : result) {
            System.out.println(String.join("\t\t --> ", tuple._2, ArrayUtils.toString(tuple._1)));
        }
    }
}
