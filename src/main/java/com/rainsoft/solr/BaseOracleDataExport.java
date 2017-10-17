package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Oracle数据导入Solr基础信息类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleDataExport.class);

    //一次写入文件的数据量
    protected static final int writeSize = 100000;
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    //数字输出格式
    protected static NumberFormat numberFormat = NumberFormat.getNumberInstance();

    //创建Spring Context
    protected static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    //创建Solr客户端
    protected static SolrClient client = SolrUtil.getClusterSolrClient();

    public static DateFormat hourDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
    public static DateFormat dateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static final DateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //一次处理多少小时的数据
    public static final int hourOffset = ConfigurationManager.getInteger("oracle.capture.time.batch");

    //导入记录文件
    static File recordFile;

    //导入记录
    public static Map<String, String> recordMap = new HashMap<>();

    public static final String SUCCESS_STATUS = "success";
    public static final String FAIL_STATUS = "fail";
    private static JavaSparkContext sc = null;

    public static JavaSparkContext getSparkContext() {
        if (sc == null || sc.env().isStopped()) {
            SparkConf conf = new SparkConf()
                    .setAppName(BaseOracleDataExport.class.getSimpleName())
                    .set("spark.ui.port", "4050")
                    .setMaster("local");
            sc = new JavaSparkContext(conf);
        }

        return sc;

    }

    static {
        //导入记录
        String importRecordFile = "createIndexRecord/index-record.txt";
        //转换文件分隔符,使在Window和Linux下都可以使用
        String convertImportRecordFile = importRecordFile.replace("/", FILE_SEPARATOR).replace("\\", FILE_SEPARATOR);
        //创建导入记录文件
        recordFile = FileUtils.getFile(convertImportRecordFile);
        File parentFile = recordFile.getParentFile();

        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }

        if (!recordFile.exists()) {
            try {
                recordFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //导入记录
        List<String> recordsList = null;
        try {
            recordsList = FileUtils.readLines(recordFile, "utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //导入记录转为Map
        assert recordsList != null;
        for (String record : recordsList) {
            String[] kv = record.split("\t");
            recordMap.put(kv[0], kv[1]);
        }

        logger.info("程序初始化完成...");
    }

    /**
     * Oracle内容表数据导入到Solr
     * @param javaRDD
     * @param task
     */
    public static void oracleContentTableDataExportSolr(JavaRDD<String[]> javaRDD, String task) {
        //字段名数组
        String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getOracleContentTableName(task));
        //捕获时间的位置
        int captureTimeIndex = ArrayUtils.indexOf(columns, BigDataConstants.CAPTURE_TIME);

        javaRDD.foreachPartition(
                (VoidFunction<Iterator<String[]>>) iterator -> {
                    List<SolrInputDocument> docList = new ArrayList<>();
                    while (iterator.hasNext()) {
                        //数据列数组
                        String[] str = iterator.next();
                        SolrInputDocument doc = new SolrInputDocument();
                        String id = UUID.randomUUID().toString().replace("-", "");

                        //ID
                        doc.addField("ID", id);
                        //docType
                        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task));

                        for (int i = 0; i < str.length; i++) {
                            String colValue = str[i];
                            //如果字段值为空跳过
                            if (StringUtils.isBlank(colValue))
                                continue;

                            //如果字段下标越界,跳出循环
                            if (i >= columns.length)
                                break;

                            //sid
                            if ("id".equalsIgnoreCase(columns[i]))
                                doc.addField("SID", colValue);
                            else
                                doc.addField(columns[i].toUpperCase(), colValue);
                        }

                        String aaa = str[captureTimeIndex];
                        //capture_time
                        doc.addField(
                                BigDataConstants.CAPTURE_TIME.toLowerCase(),
                                TIME_FORMAT.parse(aaa).getTime());

                        docList.add(doc);
                        if (docList.size() >= writeSize) {
                            client.add(docList, 10000);
                            logger.info("提交一次到Solr完成...");
                            docList.clear();
                        }

                    }
                    if (docList.size() > 0) {
                        //写入Solr
                        client.add(docList, 1000);

                        logger.info("---->写入Solr成功");
                    } else {
                        logger.info("{} 此Spark Partition 数据为空", task);
                    }
                }
        );

        logger.info("####### {}的BCP数据索引Solr完成 #######", task);

    }

    /**
     * 数据导入结果处理
     * 将导入结果记录到文件
     *
     * @param type        任务类型
     * @param captureTime 捕获日期
     * @param flat        导入结果
     * @throws IOException 文件写入失败
     */
    static void recordImportResult(String type, String captureTime, boolean flat) {
        //数据索引结果成功或者失败写入记录文件,
        String newRecords;
        if (flat) {
            recordMap.put(captureTime + "_" + type, SUCCESS_STATUS);
            newRecords = captureTime + "_" + type + "\t" + SUCCESS_STATUS + "\r\n";
        } else {
            recordMap.put(captureTime + "_" + type, FAIL_STATUS);
            newRecords = captureTime + "_" + type + "\t" + FAIL_STATUS + "\r\n";
            logger.error("当天数据导入失败");
        }

        //写入导入记录文件
        writeRecordIntoFile(newRecords);

        logger.info("{} : {} 的数据,索引完成", type, captureTime);
    }

    /**
     * 导入记录写入文件
     * @param record
     */
    static void writeRecordIntoFile(String record) {
        //写入导入记录文件
        try {
            FileUtils.writeStringToFile(recordFile, record, "utf-8", true);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("写入失败:{}", record);
            System.exit(-1);
        }

    }
}
