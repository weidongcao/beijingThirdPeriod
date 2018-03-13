package com.rainsoft.solr;

import com.google.common.base.Optional;
import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Oracle数据导入Solr基础信息类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleDataExport.class);

    //一次写入Solr的数据量
    private static final int writeSize = ConfigurationManager.getInteger("commit.solr.count");
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    //创建Spring Context
    protected static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
    //创建Solr客户端
    protected static SolrClient client = (SolrClient) context.getBean("solrClient");
    private static final DateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //秒表计时
    static StopWatch watch = new StopWatch();
    //导入记录文件
    private static File recordFile;
    //导入记录
    static Map<String, Tuple2<String, Long>> recordMap = new HashMap<>();
    //是否导入到HBase
    private static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");
    //SparkContext
    private static JavaSparkContext sc = null;
    //导入记录中最慢的导入记录
    private static Date earliestRecordTime = null;

    /**
     * 应用初始化
     */
    static {
        init();
    }

    /**
     * 初始化SparkContext
     * @return
     */
    private static JavaSparkContext getSparkContext() {
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
     * 内容表从Oracle实时导出到Solr、HBase
     *
     * @param list   数据列表
     * @param task   任务名
     * @param period 时间段
     */
    static void exportRealTimeData(List<String[]> list, String task, Tuple2<String, String> period) {
        if (list.size() > 0) {
            //数据导出到Solr、HBase
            exportData(list, task);
        } else {
            logger.info(
                    "Oracle数据库 {} 表在{} 至 {} 时间段内没有数据",
                    NamingRuleUtils.getOracleContentTableName(task),
                    period._1,
                    period._2
            );
            //如果最近两个小时没有数据的话休息5分钟
            if (DateUtils.addHours(earliestRecordTime, 2).after(new Date())) {
                ThreadUtils.programSleep(300);
            }
        }
        //更新导入最慢的导入时间
        earliestRecordTime = compareEarliestRecordTime(earliestRecordTime, period._2);
        //导入记录写入Map
        recordMap.put(NamingRuleUtils.getRealTimeOracleRecordKey(task), period._2());
        //导入记录写入文件
        overwriteRecordFile();
        //停止计时
        watch.stop();
        //重置计时器
        watch.reset();
    }

    private static void exportData(List<String[]> list, String task) {
        //根据数据量决定启动多少个线程
        int threadNum = list.size() / 200000 + 1;
        JavaRDD<Row> javaRDD = getSparkContext().parallelize(list, threadNum)
                .map(
                        (Function<String[], Row>) RowFactory::create
                );
        //数据持久化
        javaRDD.cache();
        //导入Solr
        export2Solr(javaRDD, task);
        //导入HBase
        if (isExport2HBase) {
            export2HBase(javaRDD, task);
        }
    }

    /**
     * Oracle内容表数据导入到Solr
     *
     * @param javaRDD JavaRDD<String[]>
     * @param task    任务名
     */
    private static void export2Solr(JavaRDD<Row> javaRDD, String task) {
        //字段名数组
        String[] columns = FieldConstants.ORACLE_TABLE_COLUMN_MAP.get(NamingRuleUtils.getOracleContentTableName(task));
        javaRDD.foreachPartition(
                (VoidFunction<Iterator<Row>>) iterator -> {
                    List<SolrInputDocument> docList = new ArrayList<>();
                    Optional<Object> importTime = Optional.absent();
                    while (iterator.hasNext()) {
                        //数据列数组
                        Row row = iterator.next();
                        SolrInputDocument doc = new SolrInputDocument();
                        String id = UUID.randomUUID().toString().replace("-", "");

                        //ID
                        doc.addField("ID", id);
                        //docType
                        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task));
                        for (int i = 0; i < row.length(); i++) {
                            //如果字段下标越界,跳出循环
                            if (i >= columns.length)
                                break;
                            if ("import_time".equals(columns[i])) {
                                importTime = Optional.of(row.getString(i));
                            }
                            SolrUtil.addSolrFieldValue(doc, columns[i], row.getString(i));
                        }
                        docList.add(doc);
                        //docList的size达到指定大小时写入到Solr
                        //如果是集群版Solr的话根据捕获时间动态写入到相应的Collection
                        SolrUtil.submitToSolr(client, docList, writeSize, importTime);
                    }
                    SolrUtil.submitToSolr(client, docList, 0, importTime);
                }
        );
        logger.info("####### {}的数据索引Solr完成 #######", NamingRuleUtils.getOracleContentTableName(task));
    }

    /**
     * 数据导出到HBase
     *
     * @param javaRDD JavaRDD<String[]> javaRDD
     * @param task    任务名
     */
    private static void export2HBase(JavaRDD<Row> javaRDD, String task) {
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
                        NamingRuleUtils.getOracleContentTableName(task)
                ),
                rowkeyColumn
        );
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task);
        logger.info("Oracle {} 数据写入HBase完成", task);
    }

    private static void overwriteRecordFile() {
        //导入记录Map转List
        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());
        //对转换后的导入记录List进行排序
        Collections.sort(newRecordList);
        //导入记录List转String
        String newRecords = StringUtils.join(newRecordList, "\r\n");
        //写入导入记录文件
        appendRecordIntoFile(newRecords, false);
    }

    /**
     * 导入记录写入到文件
     *
     * @param records 导入记录
     * @param append  是否是追加
     */
    private static void appendRecordIntoFile(String records, boolean append) {
        //写入导入记录文件
        try {
            FileUtils.writeStringToFile(recordFile, records, "utf-8", append);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("写入失败:{}", records);
            System.exit(-1);
        }
    }

    /**
     * 更新导入记录中导入最慢的导入时间
     * 此方法主要用于控制大表与小表导入速度的问题
     * 如果导入慢的时间与recordMap都为空，则把新的时间赋给最慢的时间
     * 如果最慢的时间为空，从导入记录里查找最早的导入时间
     * 如果都不为空，则比较原来最慢的时间与新的时间的大小，如果新的时间比它早，则赋值给它
     *
     * @param slowestTime 导入记录中导入最慢的时间
     * @param dateText 新的时间
     * @return slowestTime 导入记录中导入最慢的时间
     */
    private static Date compareEarliestRecordTime(Date slowestTime, String dateText) {
        Date date = DateUtils.stringToDate(dateText, "yyyy-MM-dd HH:mm:ss");
        //如果slowestDate和导入记录都是空的,则把新的时间赋给slowestTime
        if ((null == slowestTime) && (recordMap.isEmpty())) {
            slowestTime = date;
        } else if ((null == slowestTime) && (!recordMap.isEmpty())) {   //导入记录为空
            //记录记录不为空，从导入记录是查找最早的导入时间
            for (Tuple2<String, Long> tuple : recordMap.values()) {
                Date record = DateUtils.stringToDate(tuple._1, "yyyy-MM-dd HH:mm:ss");
                //更新导入最慢的记录
                slowestTime = compareEarliestRecordWithTime(slowestTime, record);
            }
        } else {
            //比较导入最慢的时间与新的时间
            slowestTime = compareEarliestRecordWithTime(slowestTime, date);
        }
        return slowestTime;
    }

    /**
     * 比较两个时间，
     * 如果原来的时间为空，把新时间赋给它
     * 如果新时间早于原来的时间，把新时间赋给原来的时间
     * 数据抽取的时候有的数据抽得快，比如说小表，有的数据抽取得慢，比如说大表
     * 通过此方法得到导入最慢的时间
     * @param oldDate 原来的时间
     * @param newDate 新的时间
     */
    private static Date compareEarliestRecordWithTime(Date oldDate, Date newDate) {
        if (null == oldDate) {
            oldDate = newDate;
        } else {
            if (newDate.before(oldDate)) {
                oldDate = newDate;
            }
        }
        return oldDate;
    }

    /**
     * 初始化程序
     * 主要是初始化导入记录
     */
    private static void init() {
        //导入记录
        String importRecordFile = "createIndexRecord/index-record.txt";
        //转换文件分隔符,使在Window和Linux下都可以使用
        String convertImportRecordFile = importRecordFile.replace("/", FILE_SEPARATOR).replace("\\", FILE_SEPARATOR);
        //创建导入记录文件
        recordFile = FileUtils.getFile(convertImportRecordFile);
        File parentFile = recordFile.getParentFile();

        //如果路径不存在则创建路径
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        //如果导入记录的文件不存在则创建此文件
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
            //导入记录的格式是: key importTimeTimestamp_maxId
            String[] kv = record.split("\t");
            String[] timeAndId = kv[1].split("^");
            recordMap.put(kv[0], new Tuple2<>(timeAndId[0], Long.valueOf(timeAndId[1])));
        }

        /*
         * 如果还没有导入记录，从3个月前开始导入,并添加对应记录
         */
        boolean writeFlat = false;
        for (String task : FieldConstants.DOC_TYPE_MAP.keySet()) {
            //导入记录的key
            String recordKey = NamingRuleUtils.getRealTimeOracleRecordKey(task);
            //不包含任务导入记录则添加,从3个月前开始
            if (recordMap.containsKey(recordKey) != true) {
                writeFlat = true;
                //从3个月前开始
                Date date = DateUtils.truncate(new Date(), Calendar.DATE);
                //三个月前的日期
                date = DateUtils.addMonths(date, -3);
                //获取三个月前最小的ID
                String recordValue = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");

                recordMap.put(recordKey, recordValue);
            }
        }
        if (writeFlat == true) {
            logger.info("没有导入记录,设置程序从3个月前开始导入");
            overwriteRecordFile();
        }
        logger.info("程序初始化完成...");
    }

    /**
     * 根据任务类型获取抽取记录文件中相应任务抽取到的ID
     *
     * @param task
     * @return ID
     */
    public static Optional<Long> getTaskStartId(String task) {
        String recordKey = NamingRuleUtils.getRealTimeOracleRecordKey(task);
        Tuple2<String, Long> recordValue = recordMap.get(recordKey);

        if (null != recordValue) {
            return Optional.of(recordValue._2);
        } else {
            return Optional.absent();
        }

    }
}
