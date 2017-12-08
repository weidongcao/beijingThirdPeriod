package com.rainsoft.utils;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Spark常用工具类
 * Created by CaoWeiDong on 2017-12-08.
 */
public class SparkUtils {

    //SparkSession
    public static SparkSession spark = SparkSession.builder()
            .appName("云辰信息数据管理综合平台")
            .master("local[4]")
            .getOrCreate();

    /**
     * 获取SparkContext
     *
     * @return SparkContext
     */
    public static JavaSparkContext getSparkContext() {
        JavaSparkContext jsc = null;
        if (jsc == null || jsc.env().isStopped()) {
            jsc = new JavaSparkContext(spark.sparkContext());
        }

        return jsc;

    }

    public static JavaRDD<Row> bcpDataAddRowkey(List<String> lines, String task) {

        JavaRDD<String> originalRDD = getSparkContext().parallelize(lines);

        /*
         * 对BCP文件数据进行基本的处理
         * 生成ID(HBase的RowKey，Solr的Sid)
         * 将ID作为在第一列
         *
         */
        JavaRDD<Row> valuesRDD = originalRDD.mapPartitions(
                (FlatMapFunction<Iterator<String>, Row>) iter -> {
                    //数据list
                    List<Row> list = new ArrayList<>();
                    //rowkey前缀
                    String prefixRowKey = SolrUtil.createRowKeyPrefix(new Date());
                    //rowkey标识符集合
                    List<String> ids = new ArrayList<>();
                    while (iter.hasNext()) {
                        String str = iter.next();
                        //字段值数组
                        String[] fields = str.split(BigDataConstants.BCP_FIELD_SEPARATOR);
                        //生成 rowkey&id
                        String rowKey = prefixRowKey + SolrUtil.createRowkeyIdentifier(ids);
                        //rowkey添加到字段值数组
                        fields = ArrayUtils.add(fields, 0, rowKey);
                        //添加到值列表
                        list.add(RowFactory.create(fields));
                    }
                    return list.iterator();
                }
        );

        return valuesRDD;
    }
}
