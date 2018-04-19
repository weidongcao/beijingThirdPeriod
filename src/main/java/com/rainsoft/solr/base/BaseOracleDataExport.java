package com.rainsoft.solr.base;

import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingUtils;
import com.rainsoft.utils.ThreadUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Oracle数据导入Solr基础信息类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleDataExport.class);

    //一次写入Solr的数据量
    static final int writeSize = ConfigurationManager.getInteger("commit.solr.count");
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    //创建Spring Context
    public static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
    //创建Solr客户端
    protected static SolrClient client = (SolrClient) context.getBean("solrClient");
    //秒表计时
    static StopWatch watch = new StopWatch();
    static Integer threadSleepSeconds = ConfigurationManager.getInteger("thread.sleep.seconds");
    //导入记录文件
    protected static File recordFile;
    //导入记录
    static Map<String, Long> recordMap = new HashMap<>();
    //不同的任务一次抽取的数据量，用于控制程序空转的情况（从Oracle中没有抽取到数据）
    static Map<String, Integer> extractTastCount = new HashMap<>();
    //是否导入到HBase
    static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");
    //SparkContext
    private static JavaSparkContext sc = null;

    /*
     * 应用初始化
     */
    static {
        init();
    }

    /**
     * 初始化程序
     * 主要是初始化导入记录
     */
    private static void init() {
        //导入记录
        String extractRecordFile = "createIndexRecord/index-record.txt";
        //转换文件分隔符,使在Window和Linux下都可以使用
        String recordFile = extractRecordFile.replace("/", FILE_SEPARATOR).replace("\\", FILE_SEPARATOR);
        //创建导入记录文件
        BaseOracleDataExport.recordFile = FileUtils.getFile(recordFile);
        File parentFile = BaseOracleDataExport.recordFile.getParentFile();

        //如果路径不存在则创建路径
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        //如果导入记录的文件不存在则创建此文件
        if (!BaseOracleDataExport.recordFile.exists()) {
            try {
                BaseOracleDataExport.recordFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //导入记录
        List<String> recordsList = null;
        try {
            recordsList = FileUtils.readLines(BaseOracleDataExport.recordFile, "utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //导入记录转为Map
        assert recordsList != null;
        for (String record : recordsList) {
            //导入记录的格式是: key importTimeTimestamp_maxId
            String[] kv = record.split("\t");
            recordMap.put(kv[0], Long.valueOf(kv[1]));
        }

        logger.info("程序初始化完成...");
    }

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
                FieldConstants.ORACLE_TABLE_COLUMN_MAP.get(
                        NamingUtils.getTableName(task)
                ),
                rowkeyColumn
        );
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task);
        logger.info("Oracle {} 数据写入HBase完成", task);
    }

    private static void overwriteRecordFile(File file, Map<String, Long> map) {
        //导入记录Map转List
        List<String> newRecordList = map.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "\t" + entry.getValue()).sorted().collect(Collectors.toList());
        //对转换后的导入记录List进行排序
        //导入记录List转String
        String newRecords = StringUtils.join(newRecordList, "\r\n");
        //写入导入记录文件
        appendRecordIntoFile(file, newRecords, false);
    }

    /**
     * 导入记录写入到文件
     *
     * @param records 导入记录
     * @param append  是否是追加
     */
    private static void appendRecordIntoFile(File file, String records, boolean append) {
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
     * 根据任务类型获取抽取记录Map中相应任务抽取到的ID
     *
     * @param map  抽取记录Map
     * @param task 任务类型
     * @return ID
     * @time 2018-03-14 11:46:28
     */
    protected static Optional<Long> getTaskStartId(Map<String, Long> map, String task) {
        String recordKey = NamingUtils.getOracleRecordKey(task);
        return Optional.ofNullable(map.get(recordKey));
    }

    /**
     * 数据抽取结果需要做的事情
     * 1. 抽取的最大ID写入抽取记录Map
     * 2. 抽取的最大ID写入抽取记录文件
     * 3. 停止计时
     * 4. 重置计时器
     *
     * @param task   任务类型
     * @param lastId 当前任务抽取的最大(最后一个)ID，后面再抽取的时候就从下一个ID开始抽取
     */
    public static void extractTaskOver(String task, Long lastId) {
        //导入记录写入Map
        recordMap.put(NamingUtils.getOracleRecordKey(task), lastId);
        //导入记录写入文件
        overwriteRecordFile(recordFile, recordMap);
        //停止计时
        watch.stop();
        //重置计时器
        watch.reset();
    }

    /**
     * 根据数据的关键字段生成32位加密MD5作为唯一ID
     * 不同的字段之间以下划线分隔
     *
     * @param row    数据Row
     * @param indexs 关键字段下标
     * @return
     */
    public static String getMd5ByKeyFields(Row row, List<Integer> indexs) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < indexs.size(); i++) {
            sb.append(row.getString(i)).append("_");
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return DigestUtils.md5Hex(sb.toString());
    }
}
