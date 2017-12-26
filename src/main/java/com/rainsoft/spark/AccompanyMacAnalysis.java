package com.rainsoft.spark;

import com.rainsoft.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Date;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 伴随分析
 * 指定Mac地址
 * 指定设备范围
 * 分析哪个Mac是指定Mac的同伴
 * Created by CaoWeiDong on 2017-12-14.
 */
public class AccompanyMacAnalysis implements Serializable{
    public static DecimalFormat df = new DecimalFormat("0.00");
    public static DateFormat dateFormat = new SimpleDateFormat("YYYY-mm-dd HH:MM:SS");
    public static void main(String[] args) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        SparkSession spark = SparkSession.builder()
                .appName(AccompanyMacAnalysis.class.getSimpleName())
                .master("local[4]")
                .getOrCreate();

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

        JavaPairRDD<String, String> macToEquipmentRDD = equipmentDistinctMacsRAD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, List<String>>, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, List<String>> tuple) throws Exception {
                        List<Tuple2<String, String>> list = new ArrayList<>();
                        List<String> macs = tuple._2;
                        for (String mac : macs) {
                            list.add(new Tuple2<>(mac, tuple._1));
                        }

                        return list.iterator();
                    }
                }
        );

        //两个人在不同的设备下出现的个数
        JavaPairRDD<String, List<String>> pairMacAppearEquipmentsRAD = macToEquipmentRDD.aggregateByKey(
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

        //再次翻转，以出现的设备列表为key，以两个人的mac地址为Value,为下面的排序做准备
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

        JavaPairRDD<List<String>, String> resultRDD = jsc.parallelizePairs(result);

        JavaRDD<Row> dataResultRDD = resultRDD.map(
                (Function<Tuple2<List<String>, String>, Row>) tuple -> {
                    List<Object> list = new ArrayList<>();
                    list.add("ssssssssssss");
                    list.add(tuple._2);
//                    list.add(StringUtils.join(tuple._1, ","));
                    list.add(tuple._1.size());
                    list.add(tuple._1.size());
                    list.add((float) tuple._1.size() / 100);

                    list.add(new Date(new java.util.Date().getTime()));
                    list.add(1);
                    list.add(new Date(new java.util.Date().getTime()));

                    return RowFactory.create(list.toArray());
                }
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("ENDING_MAC", DataTypes.StringType, true),
                DataTypes.createStructField("ENDING_MAC_EXTRA", DataTypes.StringType, true),
                DataTypes.createStructField("EQUIP_CNT", DataTypes.IntegerType, true),
                DataTypes.createStructField("CNT", DataTypes.IntegerType, true),
                DataTypes.createStructField("WEIGHT", DataTypes.FloatType, true),
                DataTypes.createStructField("STATS_DATE", DataTypes.DateType, true),
                DataTypes.createStructField("STATS_TYPE", DataTypes.IntegerType, true),
                DataTypes.createStructField("INSERT_TIME", DataTypes.DateType, true)

        ));
        Dataset<Row> dataResultDF = spark.createDataFrame(dataResultRDD, schema);

        SparkUtils.writeIntoOracle(dataResultDF, "COLLECT_FRI_WEIGHT");
    }
}
