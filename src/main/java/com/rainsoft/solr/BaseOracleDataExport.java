package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.inter.ContentDaoBaseInter;
import com.rainsoft.inter.ISecDaoBaseInter;
import com.rainsoft.inter.InfoDaoBaseInter;
import com.rainsoft.utils.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
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
    //秒表计时
    private static StopWatch watch = new StopWatch();
    //导入记录文件
    private static File recordFile;
    //导入记录
    private static Map<String, Long> recordMap = new HashMap<>();
    //不同的任务一次抽取的数据量，用于控制程序空转的情况（从Oracle中没有抽取到数据）
    private static Map<String, Integer> extractTastCount = new HashMap<>();
    //是否导入到HBase
    private static boolean isExport2HBase = ConfigurationManager.getBoolean("is.export.to.hbase");
    //SparkContext
    private static JavaSparkContext sc = null;

    /*
     * 应用初始化
     */
    static {
        init();
    }

    /**
     * 初始化SparkContext
     *
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
     * 从数据库抽取数据到Solr、HBase
     * 主要做三件事件：
     * 1. 抽取并导入数据
     * 2. 监控抽取
     * 3. 记录抽取情况
     *
     * 根据开始ID，及步长从数据库里取指定数量的数据
     * @param task 任务类型
     * @param dao 数据库连接dao
     * @param months 历史数据抽取的的数据量(以月为单位)
     */
    public static void extract(String task, ISecDaoBaseInter dao, Optional<Integer> months) {
        //监控执行情况
        watch.start();

        Optional<Long> id = getTaskStartId(recordMap, task);
        //首次抽取获取开始的ID
        if (!id.isPresent())
            id = getStartID(dao, months);

        //根据起始ID及步长从数据库里查询指定数据的数据
        List<String[]> list = dao.getDatasByStartIDWithStep(id);
        //记录从Oracle取到了多少数据
        extractTastCount.put(task, list.size());
        logger.info("从 {} 取到到 {} 条数据", NamingRuleUtils.getOracleContentTableName(task), list.size());

        if (list.size() > 0) {
            extractDataToSolrHBase(list, task);
        } else {
            //如果所有任务抽取的数据都为0则程序休眠10分钟
            int cnt = extractTastCount.values().stream().mapToInt(i -> i).sum();
            if (cnt == 0){
                //Windows下主要用于测试，休眠的时间短些，Linux下主要用于生产，休眠的时间长些
                int seconds;
                String os = System.getProperty("os.name");
                if (os.toLowerCase().startsWith("win")) {
                    seconds = 30;
                } else {
                    seconds = 300;
                }
                logger.info("Oracle数据所有表都没有抽取到数据，开始休眠，休眠时间: {}", seconds);
                ThreadUtils.programSleep(seconds);
            }
        }

        //导入记录写入Map
        recordMap.put(NamingRuleUtils.getOracleRecordKey(task), id.get() + list.size());
        //导入记录写入文件
        overwriteRecordFile(recordFile, recordMap);
        //停止计时
        watch.stop();
        //重置计时器
        watch.reset();
    }

    /**
     * 把抽取的数据写入Solr、HBase
     * @param list 抽取的数据
     * @param task 任务类型
     */
    private static void extractDataToSolrHBase(List<String[]> list, String task) {
        logger.info("{}表数据抽取任务开始", NamingRuleUtils.getOracleContentTableName(task));
        //根据数据量决定启动多少个线程
        int threadNum = list.size() / 200000 + 1;
        JavaRDD<Row> javaRDD = getSparkContext().parallelize(list, threadNum)
                .map(
                        (Function<String[], Row>) RowFactory::create
                );
        //数据持久化
        javaRDD.persist(StorageLevel.MEMORY_ONLY());
        //导入Solr
        export2Solr(javaRDD, task);
        //导入HBase
        if (isExport2HBase) {
            export2HBase(javaRDD, task);
        }
        javaRDD.unpersist();
        logger.info("{}表一次数据抽取任务完成...", NamingRuleUtils.getOracleContentTableName(task));
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
                    //导入时间，集群版Solr需要根据导入时间确定具体导入到哪个Collection
                    java.util.Optional<Object> importTime = java.util.Optional.empty();
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
                            if ("import_time".equalsIgnoreCase(columns[i])) {
                                importTime = Optional.of(row.getString(i));
                            }
                            SolrUtil.addSolrFieldValue(doc, columns[i].toUpperCase(), row.getString(i));
                        }
                        docList.add(doc);
                        //docList的size达到指定大小时写入到Solr
                        //如果是集群版Solr的话根据捕获时间动态写入到相应的Collection
                        SolrUtil.submitToSolr(client, docList, writeSize, importTime);
                    }
                    SolrUtil.submitToSolr(client, docList, 1, importTime);
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
            recordMap.put(kv[0], Long.valueOf(kv[1]));
        }

        logger.info("程序初始化完成...");
    }

    /**
     * 根据任务类型获取抽取记录Map中相应任务抽取到的ID
     *
     * @param map  抽取记录Map
     * @param task 任务类型
     * @return ID
     * @time 2018-03-14 11:46:28
     */
    private static Optional<Long> getTaskStartId(Map<String, Long> map, String task) {
        String recordKey = NamingRuleUtils.getOracleRecordKey(task);
        return Optional.ofNullable(map.get(recordKey));
    }


    /**
     * 从数据库中获取从指定时间开始最小的ID
     * 如果是内容的,比如说Bbs，Email，Ftp，Http，Imchat等从三个月前开始最小的ID
     * 如果是信息表的，比如说真实、虚拟、场所等没有时间限制，从最小的开始导入
     *
     * @param dao    数据库连接DAO层
     * @param months 　从Oracle数据库表中抽取指定时间（单位为月）的数（从当前时间算起，抽到当前时间不会停，会继续抽新进来的数据）
     * @return 抽取起始ID
     */
    private static Optional<Long> getStartID(ISecDaoBaseInter dao, Optional<Integer> months) {
        Optional<Long> id = Optional.empty();
        if (dao instanceof ContentDaoBaseInter) {
            //三个月前的日期
            String dateText = DateFormatUtils.DATE_FORMAT.format(DateUtils.addMonths(new Date(), months.get()));
            //从三个月前起的ID
            id = ((ContentDaoBaseInter) dao).getMinIdFromDate(Optional.of(dateText));
        } else if (dao instanceof InfoDaoBaseInter) {
            //最小的ID，没有时间限制
            id = ((InfoDaoBaseInter) dao).getMinId();
        }
        return id;
    }
}
