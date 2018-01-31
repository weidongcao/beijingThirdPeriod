package com.rainsoft.utils;

import com.rainsoft.BigDataConstants;
import com.rainsoft.conf.ConfigurationManager;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

import java.util.*;

/**
 * Spark常用工具类
 * Created by CaoWeiDong on 2017-12-08.
 */
public class SparkUtils {

    /**
     * 获取SparkContext
     *
     * @return SparkContext
     */
    public static JavaSparkContext getSparkContext(SparkSession spark) {
        JavaSparkContext jsc = null;
        if (jsc == null || jsc.env().isStopped()) {
            jsc = new JavaSparkContext(spark.sparkContext());
        }
        return jsc;
    }

    /**
     * 给BCP文件的数据添加唯一主键
     * @param dataRDD JavaRDD<String>
     * @param task
     * @return JavaRDD<Row>
     */
    public static JavaRDD<Row> bcpDataAddRowkey(JavaRDD<String> dataRDD, String task) {
        /*
         * 对BCP文件数据进行基本的处理
         * 生成ID(HBase的RowKey，Solr的Sid)
         * 将ID作为在第一列
         *
         */
        JavaRDD<Row> valuesRDD = dataRDD.mapPartitions(
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

    public static Properties getOracleConnectionProperties(String username, String passwd, String driver, String tablename) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Properties prop = new Properties();
        prop.put("user", username);
        prop.put("password", passwd);
        prop.put("dbtable", tablename);
        Class.forName(driver).newInstance();

        return prop;
    }

    public static void writeIntoOracle(Dataset<Row> df, String tableName) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        String username = ConfigurationManager.getProperty("oracle.username");
        String passWord = ConfigurationManager.getProperty("oracle.password");
        String driver = ConfigurationManager.getProperty("oracle.driver");
        String uri = ConfigurationManager.getProperty("oracle.url");

        JdbcDialect jdbcDialect = new OracleJdbcDialects();
        JdbcDialects.registerDialect(jdbcDialect);

        Properties prop = getOracleConnectionProperties(username, passWord, driver, tableName);

        JdbcUtils.saveTable(df, uri, tableName, prop);
    }
}
