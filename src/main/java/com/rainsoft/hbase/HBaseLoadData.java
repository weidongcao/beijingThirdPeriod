package com.rainsoft.hbase;

import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 程序说明：
 * 通过Spark2.0把本地的数据导入到HBase里
 * Created by CaoWeiDong on 2017-12-17.
 */
public class HBaseLoadData {
    //HBase的表名
    public static final String TABLE_NAME = "track";
    //HBase列簇
    public static final String CF = "info";
    //HBase字段名
    public static final String[] columns = new String[]{"commonity", "mac"};

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName(HBaseLoadData.class.getSimpleName())
                .master("local[2]")
                .getOrCreate();

        //读取本地数据文件
        JavaRDD<Row> dataRDD = spark.read().csv("file:///E:\\data\\track.csv").javaRDD();

        //将数据添加RowKey及进行二次排序
        JavaPairRDD<RowkeyColumnSecondarySort, String> hbasePairRDD = dataRDD.flatMapToPair(
                new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterator<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();

                        String rowkey = SolrUtil.createRowkey();
                        for (int i = 0; i < row.length(); i++) {
                            String column = columns[i];
                            String value = row.getString(i);
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, column), value));
                        }

                        return list.iterator();
                    }
                }
        ).sortByKey();

        //生成生成HFile文件并加载到HBase
        HBaseUtils.writeData2HBase(hbasePairRDD, TABLE_NAME, CF, "/tmp/data");
    }
}
