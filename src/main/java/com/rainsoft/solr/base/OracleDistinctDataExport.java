package com.rainsoft.solr.base;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.inter.InfoDaoInter;
import com.rainsoft.utils.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by CaoWeiDong on 2018-04-18.
 */
public class OracleDistinctDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(OracleDistinctDataExport.class);
    //导入记录
    static Map<String, Date> recordMap = new HashMap<>();
    //导入记录文件
    protected static File recordFile;

    // 应用初始化
    static {
        init();
    }

    /**
     * 初始化程序
     * 主要是初始化导入记录
     */
    /**
     * 初始化程序
     */
    private static void init() {
        //导入记录文件
        recordFile = createOrGetFile("createIndexRecord/record_distinct.txt");

        //导入记录Map
        Optional<Map<String, String>> map = readFileToMap(recordFile, "utf-8", "\t");

        if (map.isPresent())
            map.get().forEach(
                    (key, value) -> recordMap.put(key, DateUtils.stringToDate(value, "yyyy-MM-dd HH:mm:ss"))
            );

        logger.info("程序初始化完成...");
    }

    /**
     * 从数据库抽取数据到Solr、HBase
     * 主要做三件事件：
     * 1. 抽取并导入数据
     * 2. 监控抽取
     * 3. 记录抽取情况
     * <p>
     * 根据开始ID，及步长从数据库里取指定数量的数据
     *
     * @param task 任务类型
     * @param dao  数据库连接dao
     */
    public static void extract(String task, InfoDaoInter dao) {
        //监控执行情况
        if (watch.isStopped())
            watch.start();

        //如果日期为空则从1970年1月1日开始抽取
        Date startTime = recordMap.get(NamingUtils.getOracleRecordKey(task));
        if (null == startTime) {
            Optional<Date> optional = dao.getMinTime();
            if (optional.isPresent())
                startTime = optional.get();
            else
                return;
        }
        int minutes = 5;
        //根据起始ID及步长从数据库里查询指定数据量的数据
        Date endTime = getEndTime(startTime, minutes);

        //如果时间间隔不到30分钟不抽取（这类的数据太少）
        Date curDate = new Date();
        Long minus = (curDate.getTime() - startTime.getTime()) / (60 * 1000);
        if (minus < minutes) {
            logger.info("与之前抽取时间间隔太短， {} 数据太少，间隔 {} 分钟再抽取", task, minutes);
            // extractTaskOver(task, endTime, recordMap, recordFile, watch);
            ThreadUtils.programSleep(3);
            return;
        }

        List<String[]> list = dao.getDataByTime(
                DateFormatUtils.DATE_TIME_FORMAT.format(startTime),
                DateFormatUtils.DATE_TIME_FORMAT.format(endTime)
        );
        logger.info("从 {} 取到到 {} 条数据", NamingUtils.getTableName(task), list.size());

        //根据不同的情况进行抽取
        extractDataOnCondition(list, task);

        //一次任务抽取完之后需要做的事情
        extractTaskOver(task, endTime, recordMap, recordFile, watch);
    }

    /**
     * Oracle内容表数据导入到Solr
     *
     * @param javaRDD JavaRDD<String[]>
     * @param task    任务名
     */
    private static void export2Solr(JavaRDD<Row> javaRDD, String task) {
        //字段名数组
        String[] columns = FieldConstants.ORACLE_TABLE_COLUMN_MAP.get(NamingUtils.getTableName(task));
        List<Integer> indexs = getKeyfieldIndexs(columns, FieldConstants.TASK_KEY_MAP.get(task));

        javaRDD.foreachPartition(
                (VoidFunction<Iterator<Row>>) iterator -> {
                    List<SolrInputDocument> docList = new ArrayList<>();
                    //导入时间，集群版Solr需要根据导入时间确定具体导入到哪个Collection
                    while (iterator.hasNext()) {
                        //数据列数组
                        Row row = iterator.next();
                        SolrInputDocument doc = new SolrInputDocument();
                        //通过MD5加密生成Sokr唯一ID
                        String id = getMd5ByKeyFields(row, indexs);

                        //ID
                        doc.addField("ID", id);
                        //docType
                        doc.addField(BigDataConstants.SOLR_DOC_TYPE_KEY, FieldConstants.DOC_TYPE_MAP.get(task));

                        for (int i = 0; i < row.length(); i++) {
                            //如果字段下标越界,跳出循环
                            if (i >= columns.length)
                                break;
                            SolrUtil.addSolrFieldValue(doc, columns[i].toUpperCase(), row.getString(i));
                        }
                        docList.add(doc);
                        //docList的size达到指定大小时写入到Solr
                        //如果是集群版Solr的话根据捕获时间动态写入到相应的Collection
                        if (writeSize <= docList.size())
                            SolrUtil.submitToSolr(client, docList, writeSize, Optional.of("yisou"));
                    }
                    SolrUtil.submitToSolr(client, docList, 1, Optional.of("yisou"));
                }
        );
        logger.info("####### {}的数据索引Solr完成 #######", NamingUtils.getTableName(task));
    }

    /**
     * 根据数据及任务类型处理不同的抽取情况
     * 如果任务有数据则进行抽取，如果没有数据的话判断是否需要休眠（所有的任务都没有数据）
     *
     * @param list 要抽取的数据
     * @param task 任务类型
     */
    private static void extractDataOnCondition(List<String[]> list, String task) {
        if (list.size() > 0) {
            logger.info("{}表数据抽取任务开始", NamingUtils.getTableName(task));
            //根据数据量决定启动多少个进程
            int threadNum = list.size() / 200000 + 1;
            JavaRDD<Row> javaRDD = getSparkContext().parallelize(list, threadNum)
                    .map((Function<String[], Row>) RowFactory::create);

            //数据持久化
            javaRDD.persist(StorageLevel.MEMORY_ONLY());

            //抽取到Solr
            export2Solr(javaRDD, task);

            //抽取到HBase
            if (isExport2HBase)
                export2HBase(javaRDD, task);

            //取消持久化
            javaRDD.unpersist();

            logger.info("{}表一次数据抽取任务完成...", NamingUtils.getTableName(task));
        }
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

    /**
     * 根据数据的关键字段生成32位加密MD5作为唯一ID
     * 不同的字段之间以下划线分隔
     *
     * @param row    数据Row
     * @param indexs 关键字段下标
     * @return 32位加密MD5
     */
    static String getMd5ByKeyFields(Row row, List<Integer> indexs) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < indexs.size(); i++) {
            sb.append(row.getString(i)).append("_");
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return DigestUtils.md5Hex(sb.toString());
    }

    /**
     * 获取一段时间的开始和结束时间
     * 开始时间从导入记录中获取
     * 结束时间如果此时间段不长的话以方法传的当前时间为准
     * 如果长的话从结束时间开始向后偏移指定的小时数
     *
     * @param minutes 偏移的小时数
     * @return 返回时间段的开始时间和结束时间
     */
    static Date getEndTime(Date startTime, int minutes) {
        //去掉开始时间的秒数
        Date curTime = DateUtils.truncate(new Date(), Calendar.MINUTE);

        // 防止长时间没有导入,数据大量堆积的情况
        // 如果开始时间和记录的最后导入时间相距超过10天,时间时隔改为10天
        Date endTime;
        //开始时间和当前时间相差多少分钟
        long minus = (curTime.getTime() - startTime.getTime()) / (24 * 60 * 60 * 1000);

        if (minus > 10) { //如果相差10天以上一次取10天的数据
            endTime = DateUtils.addDays(startTime, 10);
        } else {
            endTime = curTime;
        }
        return endTime;
    }

    /**
     * 数据抽取结果需要做的事情
     * 1. 抽取的最大ID写入抽取记录Map
     * 2. 抽取的最大ID写入抽取记录文件
     * 3. 停止计时
     * 4. 重置计时器
     *
     * @param task  任务类型
     * @param date  任务抽取到的时间
     * @param map   更新抽取记录Map
     * @param file  更新抽取记录文件
     * @param watch 重置秒表
     */
    public static void extractTaskOver(String task, Date date, Map<String, Date> map, File file, StopWatch watch) {
        //导入记录写入Map
        map.put(NamingUtils.getOracleRecordKey(task), date);
        //导入记录写入文件
        overwriteRecordFile(file, map);
        //停止计时
        if (watch.isStarted())
            watch.stop();
        //重置计时器
        watch.reset();
    }

    private static void overwriteRecordFile(File file, Map<String, Date> map) {
        //导入记录Map转List
        List<String> newRecordList = map.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "\t" + DateFormatUtils.DATE_TIME_FORMAT.format(entry.getValue()))
                .sorted()
                .collect(Collectors.toList());
        //对转换后的导入记录List进行排序
        //导入记录List转String
        String newRecords = StringUtils.join(newRecordList, "\r\n");
        //写入导入记录文件
        appendRecordIntoFile(file, newRecords, false);
    }
}
