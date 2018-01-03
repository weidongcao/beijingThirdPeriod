package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.ThreadUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
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

    //一次写入文件的数据量
    private static final int writeSize = 100000;
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
    static Map<String, String> recordMap = new HashMap<>();

    //状态：导入成功
    private static final String SUCCESS_STATUS = "success";
    //状态：导入失败
    private static final String FAIL_STATUS = "fail";
    //是否导入到HBase
    private static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");
    //SparkContext
    private static JavaSparkContext sc = null;

    //导入记录中最慢的导入记录
    protected static Date earliestRecordTime = null;

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
            logger.info("Oracle数据库 {} 表在{} 至 {} 时间段内没有数据", NamingRuleUtils.getOracleContentTableName(task), period._1, period._2);

            //更新导入最慢的导入时间
            compareEarliestRecordTime(period._2);

            //如果最近两个小时没有数据的话休息5分钟
            if (DateUtils.addHours(earliestRecordTime, 2).after(new Date())) {
                ThreadUtils.programSleep(300);
            }
        }
        //更新导入最慢的导入时间
        compareEarliestRecordTime(period._2);

        //导入记录写入Map
        recordMap.put(NamingRuleUtils.getRealTimeOracleRecordKey(task), period._2());
        //导入记录写入文件
        overwriteRecordFile();

        //停止计时
        watch.stop();

        //程序执行完成记录日志
        taskDonelogger(task, period);

        //重置计时器
        watch.reset();

        //程序休眠
//        ThreadUtils.programSleep(3);
    }

    /**
     * Oracle内容表数据导入到Solr
     *
     * @param javaRDD JavaRDD<String[]>
     * @param task    任务名
     */
    private static void export2Solr(JavaRDD<Row> javaRDD, String task) {
        //字段名数组
        String[] columns = FieldConstants.COLUMN_MAP.get(NamingRuleUtils.getOracleContentTableName(task));
        //捕获时间的位置
        int captureTimeIndex = ArrayUtils.indexOf(columns, BigDataConstants.CAPTURE_TIME);

        javaRDD.foreachPartition(
                (VoidFunction<Iterator<Row>>) iterator -> {
                    List<SolrInputDocument> docList = new ArrayList<>();
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
                            String colValue = row.getString(i);
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

                        //如果有capture_time(小写)字段添加此字段到solr
                        if (captureTimeIndex > 0) {
                            String captureTimeValue = row.getString(captureTimeIndex);
                            //capture_time
                            doc.addField(
                                    BigDataConstants.CAPTURE_TIME.toLowerCase(),
                                    TIME_FORMAT.parse(captureTimeValue).getTime());
                        }

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
                FieldConstants.COLUMN_MAP.get(
                        NamingRuleUtils.getOracleContentTableName(task)
                ),
                rowkeyColumn
        );

        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, task);

        logger.info("Oracle {} 数据写入HBase完成", task);
    }

    /**
     * 更新导入记录文件
     *
     * @param type        任务类型
     * @param captureTime 捕获日期
     * @param flat        导入结果
     */
    static void updateRecordFile(String type, String captureTime, boolean flat) {
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
        appendRecordIntoFile(newRecords, true);

        logger.info("{} : {} 的数据,索引完成", type, captureTime);
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
     * 获取一段时间的开始和结束时间
     * 开始时间从导入记录中获取
     * 结束时间如果此时间段不长的话以方法传的当前时间为准
     * 如果长的话从结束时间开始向后偏移指定的小时数
     *
     * @param task  任务类型
     * @param hours 偏移的小时数
     * @return 返回时间段的开始时间和结束时间
     */
    static Tuple2<String, String> getPeriod(String task, int hours) {
        //去掉开始时间的秒数
        Date curTime = DateUtils.truncate(new Date(), Calendar.MINUTE);

        //从导入记录中获取最后导入的日期
        String startTime_String = recordMap.get(NamingRuleUtils.getRealTimeOracleRecordKey(task));

        Date startTime_Date = null;
        //第一次导入没有记录最后导入时间,从半年前开始导入
        if (StringUtils.isBlank(startTime_String)) {
            //将半年前的日期赋给开始时间
            startTime_Date = DateUtils.addDays(
                    //从0点开始导
                    DateUtils.truncate(curTime, Calendar.DATE),
                    //日期向前推半年
                    -180
            );
            startTime_String = TIME_FORMAT.format(startTime_Date);
        } else {
            try {
                startTime_Date = DateUtils.parseDate(startTime_String, "yyyy-MM-dd HH:mm:ss");
            } catch (ParseException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        /*
         * 防止长时间没有导入,数据大量堆积的情况
         * 如果开始时间和记录的最后导入时间相距超过10天,取从最后导入时间之后10天的数据
         */
        Date tmpDate = DateUtils.addHours(startTime_Date, hours);
        Date endTime_Date;
        if (tmpDate.before(curTime)) {
            endTime_Date = tmpDate;
        } else {
            endTime_Date = curTime;
        }
        String endTime_String = TIME_FORMAT.format(endTime_Date);

        return new Tuple2<>(startTime_String, endTime_String);
    }

    /**
     * 任务执行完成要做的事情
     *
     * @param task   任务名
     * @param period 时间段
     */
    private static void taskDonelogger(String task, Tuple2<String, String> period) {
        //记录任务执行时间
        logger.info(
                "{} 导出完成。执行时间: {}",
                task,
                DurationFormatUtils.formatDuration(watch.getTime(), "yyyy-MM-dd HH:mm:ss")
        );

        //提供查询所导入数据的条件
        try {
            logger.info(
                    "Solr 查询此数据的条件: docType:{} capture_time:[{} TO {}]",
                    FieldConstants.DOC_TYPE_MAP.get(task),
                    TIME_FORMAT.parse(period._1()).getTime(),
                    TIME_FORMAT.parse(period._2()).getTime()
            );

            //一次任务结束程序休息一分钟
            Thread.sleep(1000);
        } catch (ParseException | InterruptedException e) {
            e.printStackTrace();

            System.exit(-1);
        }
    }

    /**
     * 更新导入记录中导入最慢的导入时间
     */
    private static void compareEarliestRecordTime(String dateText) {
        Date date = DateUtils.stringToDate(dateText, "yyyy-MM-dd HH:mm:ss");
        //如果earliestRecordTime和导入记录都是空的
        if ((null == earliestRecordTime) && (recordMap.isEmpty())) {
            earliestRecordTime = date;
            return;
        } else if ((null == earliestRecordTime) && (!recordMap.isEmpty())) {
            //记录记录不为空，从导入记录是查找最早的导入时间
            for (String str : recordMap.values()) {
                Date record = DateUtils.stringToDate(str, "yyyy-MM-dd HH:mm:ss");
                compareEarliestRecordWithTime(record);
            }
        }
        compareEarliestRecordWithTime(date);
    }

    /**
     * 将最早的导入时间与指定的时间进行比较，哪个时间更早，就用哪个
     * @param date
     */
    private static void compareEarliestRecordWithTime(Date date) {
        if (null == earliestRecordTime) {
            earliestRecordTime = date;
        } else {
            if (date.before(earliestRecordTime)) {
                earliestRecordTime = date;
            }
        }
    }
}
