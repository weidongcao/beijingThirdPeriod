package com.rainsoft.hbase.oracle;

import com.rainsoft.BigDataConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.HBaseUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试通过Spark把Oracle的数据导入到HBase里
 * Created by CaoWeiDong on 2017-07-21.
 */
public class SparkImportOracleToHBase {
    private static final Logger logger = LoggerFactory.getLogger(SparkImportOracleToHBase.class);

    //Oracle连接地址
    private static final String ORACLE_URL = ConfigurationManager.getProperty("oracle_url");
    //Oracle驱动
    private static final String ORACLE_DRIVER = ConfigurationManager.getProperty("oracle_driver");
    //Oracle用户名
    private static final String ORACLE_USERNAME = ConfigurationManager.getProperty("oracle_username");
    //Oracle密码
    private static final String ORACLE_PASSWORD = ConfigurationManager.getProperty("oracle_password");

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setAppName(SparkImportOracleToHBase.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc.sc());

        //表名
        String tableName = args[0];
        //列簇名
        String cf = args[1];
        //HFile的HDFS临时存储目录
        String tmepHDFSPath = args[2];

        //Oracle分页的页数
        int pageIndex = Integer.valueOf(args[3]);
        //Oracle分页一页的数据量
        int pageSize = ConfigurationManager.getInteger("oracle_page_size");

        /*
         * Spark连接Oracle的信息
         */
        Map<String, String> jdbcMap = new HashMap<>();
//        String oracleLogin = ORACLE_URL.replace("@", ORACLE_USERNAME + "/" + ORACLE_PASSWORD + "@");
        jdbcMap.put("url", ORACLE_URL);
        jdbcMap.put("driver", ORACLE_DRIVER);
        jdbcMap.put("user", ORACLE_USERNAME);
        jdbcMap.put("password", ORACLE_PASSWORD);
        jdbcMap.put("dbtable", query(tableName, pageIndex, pageSize));

        /*
         * Spark读取Oracle
         */
        DataFrame jdbcDF = sqlContext.read().format("jdbc").options(jdbcMap).load();

        JavaRDD<Row> jdbcRDD = jdbcDF.javaRDD();
        StructType jdbcSchema = jdbcDF.schema();
        String[] fieldNames = jdbcSchema.fieldNames();
        List<Row> list = jdbcRDD.take(3);

        /*
         * 数据转换为HBase的HFile格式
         */
        JavaPairRDD<RowkeyColumnSecondarySort, String> hbasePairRDD = jdbcRDD.flatMapToPair(
                new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {

                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                        if (null == row.get(0)) {
                            return null;
                        }
                        String rowkey = row.get(0).toString().split("\\.")[0];

                        for (int i = 1; i < fieldNames.length; i++) {
                            if (null != row.get(i) && (!BigDataConstants.ROWNUM.equalsIgnoreCase(fieldNames[i]))) {
                                list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, fieldNames[i]), row.get(i).toString()));
                            }
                        }
                        return list;
                    }
                }
        ).sortByKey();


        /*
         * Spark将HFile文件写HDFS并转存入HBase
         */
        HBaseUtils.writeData2HBase(hbasePairRDD, BigDataConstants.HBASE_TABLE_PREFIX + tableName, cf, tmepHDFSPath);
    }

    /**
     * 从Oracle查询数据的sql作为Spark的dbtable
     * @param tableName 表名
     * @param pageIndex 页码
     * @param pageSize 一个有多少数据
     * @return  Spark jdbc的dbtable参数
     */
    private static String query(String tableName, int pageIndex, int pageSize) {
        String templateSql = "(SELECT * FROM (SELECT t.*, ROWNUM ${rownum} FROM ${tableName} t WHERE ROWNUM < (${pageIndex} * ${pageSize})) a WHERE a.rn >= ((${pageIndex} -1) * ${pageSize})) c";
        String dbtable = templateSql.replace("${tableName}", tableName);
        dbtable = dbtable.replace("${rownum}", BigDataConstants.ROWNUM);
        dbtable = dbtable.replace("${pageIndex}", pageIndex + "");
        dbtable = dbtable.replace("${pageSize}", pageSize + "");
        logger.info("Oracle迁移HBaseSQL：{}", dbtable);
        return dbtable;
    }
}
