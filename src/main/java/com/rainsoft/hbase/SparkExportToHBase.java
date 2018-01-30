package com.rainsoft.hbase;

import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 测试通过Spark把Oracle的数据导入到HBase里
 * Created by CaoWeiDong on 2017-07-21.
 */
public class SparkExportToHBase {
    private static final Logger logger = LoggerFactory.getLogger(SparkExportToHBase.class);

    public static void main(String[] args) throws Exception {

        //作业类型
        String taskType = args[0];
        //hdfs数据临时存储根目录
        String hdfsDataPath = args[1];
        SparkConf conf = new SparkConf()
                .setAppName(SparkExportToHBase.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        //oracle表名
        String tableName = NamingRuleUtils.getOracleContentTableName(taskType);
        //列簇名
        String cf = NamingRuleUtils.getHBaseContentTableCF();
        //HFile的HDFS临时存储目录
        String tempHDFSPath = NamingRuleUtils.getHFileTaskDir(NamingRuleUtils.getOracleContentTableName(taskType));

        InputStream in = SparkExportToHBase.class.getClassLoader().getResourceAsStream("metadata/" + tableName.toLowerCase());

        String[] fieldNames = IOUtils.toString(in, "utf-8").split("\r\n");
        JavaRDD<String> originalRDD = sc.textFile(hdfsDataPath);
        JavaRDD<String[]> fieldRDD = originalRDD.mapPartitions(
                (FlatMapFunction<Iterator<String>, String[]>) iter -> {
                    List<String[]> list = new ArrayList<>();
                    while (iter.hasNext()) {
                        String str = iter.next();
                        String[] fields = str.split("\t");
                        list.add(fields);
                    }
                    return list.iterator();
                }
        );
        /*
         * 数据转换为HBase的HFile格式
         */
        JavaPairRDD<RowkeyColumnSecondarySort, String> hbasePairRDD = originalRDD.flatMapToPair(
                (PairFlatMapFunction<String, RowkeyColumnSecondarySort, String>) line -> {

                    List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                    String[] cols = line.split("\t");
                    String rowkey = cols[0];

                    for (int i = 1; i < cols.length; i++) {
                        String value = cols[i];
                        if ((null != value) && (!"".equals(cols[i]))) {
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, fieldNames[i]), value));
                        }
                    }
                    return list.iterator();
                }
        ).sortByKey();


        /*
         * Spark将HFile文件写HDFS并转存入HBase
         */
        HBaseUtils.writeData2HBase(hbasePairRDD, "H_" + tableName, cf, tempHDFSPath);
        logger.info("写入HBase完成");
        sc.close();
    }
}
