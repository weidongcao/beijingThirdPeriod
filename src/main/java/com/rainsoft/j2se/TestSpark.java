package com.rainsoft.j2se;

import com.rainsoft.FieldConstants;
import com.rainsoft.utils.NamingRuleUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by CaoWeiDong on 2017-10-20.
 */
public class TestSpark {
    public static void main(String[] args) {

        List<String[]> list = new ArrayList<>();
        list.add(FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey("ftp")));
        list.add(FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getOracleContentTableName("ftp")));
        list.add(FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getBcpTaskKey("http")));
        list.add(FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getOracleContentTableName("http")));

        SparkSession spark = SparkSession.builder()
                .appName(TestSpark.class.getSimpleName())
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String[]> dataRDD = jsc.parallelize(list);
        JavaRDD<Row> rowRDD = dataRDD.map(
                new Function<String[], Row>() {
                    @Override
                    public Row call(String[] v1) throws Exception {
                        return RowFactory.create(ArrayUtils.add(v1, 0, "rowkey"));
                    }
                }
        );

        rowRDD.foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println(row.toString());
                    }
                }
        );
    }
}
