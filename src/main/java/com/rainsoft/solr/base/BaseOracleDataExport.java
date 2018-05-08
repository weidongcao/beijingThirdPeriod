package com.rainsoft.solr.base;

import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Oracle数据导入Solr基础信息类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleDataExport.class);

    //一次写入Solr的数据量
    static final int writeSize = ConfigurationManager.getInteger("commit.solr.count");
    //创建Spring Context
    public static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
    //创建Solr客户端
    protected static SolrClient client = (SolrClient) context.getBean("solrClient");
    static Integer threadSleepSeconds = ConfigurationManager.getInteger("thread.sleep.seconds");
    //是否导入到HBase
    static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");
    //SparkContext
    private static JavaSparkContext sc = null;
    //秒表计时
    static StopWatch watch = new StopWatch();

    /**
     * 初始化SparkContext
     */
    static JavaSparkContext getSparkContext() {
        if (sc == null || sc.env().isStopped()) {
            SparkConf conf = new SparkConf()
                    .setAppName(BaseOracleDataExport.class.getSimpleName())
                    .set("spark.ui.port", "4050")
                    .setMaster("local");
            sc = new JavaSparkContext(conf);
        }
        return sc;
    }

    /**
     * 数据导出到HBase
     *
     * @param javaRDD JavaRDD<String[]> javaRDD
     * @param task    任务名
     */
    static void export2HBase(JavaRDD<Row> javaRDD, String task) {
        //将要作为rowkey的字段，service_info表是service_code，其他表都是id
        String rowkeyColumn;
        if (task.equalsIgnoreCase("service")) {
            rowkeyColumn = "service_code";
        } else {
            rowkeyColumn = "id";
        }
        //将数据转为可以进行二次排序的形式
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = HBaseUtils.getHFileRDD(
                javaRDD,
                FieldConstants.ORACLE_TABLE_COLUMN_MAP.get(NamingUtils.getTableName(task)),
                rowkeyColumn
        );
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task);
        logger.info("Oracle {} 数据写入HBase完成", task);
    }

    /**
     * 导入记录写入到文件
     *
     * @param records 导入记录
     * @param append  是否是追加
     */
    static void appendRecordIntoFile(File file, String records, boolean append) {
        //写入导入记录文件
        try {
            FileUtils.writeStringToFile(file, records, "utf-8", append);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("写入失败:{}", records);
            System.exit(-1);
        }
    }

    /**
     * 根据指定路径创建或获取文件
     * 如果文件存在就获取，
     * 不存在就创建
     * 返回创建后的文件
     *
     * @param path 文件路径
     * @return 文件对象
     */
    static File createOrGetFile(String path) {
        //转换文件分隔符
        String filePath = path.replace("/", File.separator).replace("\\", File.separator);

        //创建导入记录文件
        File file = FileUtils.getFile(filePath);

        //判断父目录是否存在，如果不存在的话创建
        File parentFile = file.getParentFile();
        if (!parentFile.exists())
            parentFile.mkdirs();

        //判断文件是否存在,如果不存在的话创建
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file;
    }

    /**
     * 将文件内容
     * 根据指定的分隔符将文件内容转为Map
     *
     * @param file  要读取的文件
     * @param encoding 文件编码格式,如utf-8, gbk, gbk-2312
     * @param separator key, value之间的分隔符
     * @return
     */
    static Optional<Map<String, String>> readFileToMap(File file, String encoding, String separator) {
        List<String> list = null;

        try {
            list = FileUtils.readLines(file, encoding);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //导入记录转为Map
        if ((null != list) && (list.size() != 0)) {
            Map<String, String> map = new HashMap<>();
            for (String record : list) {
                String[] kv = record.split(separator);
                map.put(kv[0], kv[1]);
            }
            return Optional.ofNullable(map);
        }

        return Optional.empty();
    }

    /**
     * 根据数据的关键字段生成32位加密MD5作为唯一ID
     * 不同的字段之间以下划线分隔
     *
     * @param list 唯一标识数据的字段值列表
     * @return 32位加密MD5
     */
    public static String createIdentifyID(List<String> list) {
        String fieldValues = StringUtils.join(list, '_');
        return DigestUtils.md5Hex(fieldValues);
    }

    /**
     * 根据Row下标集合获取对应的值
     * @param row Spark-sql row
     * @param indexs Row的下标
     * @return 数据唯一标识的字段值
     */
    public static List<String> getKeyFieldValues(Row row, List<Integer> indexs) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < indexs.size(); i++) {
            int index = indexs.get(i);
            list.add(row.getString(index));
        }
        return list;
    }

    /**
     * 从字段名称数组中找出关键字段的下标
     * 返回关键字段下标的列表
     * 主要用于根据关键字段生成唯一ID
     *
     * @param fields    该类型所有字段名称
     * @param keyfields 该类型关键字段名称
     * @return 字段在
     */
    static List<Integer> getKeyfieldIndexs(String[] fields, String[] keyfields) {
        List<Integer> indexs = new ArrayList<>();
        int index;
        for (String field : keyfields) {
            index = ArrayUtils.indexOf(fields, field);
            indexs.add(index);
        }
        return indexs;
    }
}
