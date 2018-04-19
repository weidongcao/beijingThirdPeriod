package com.rainsoft.solr.base;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.inter.InfoDaoInter;
import com.rainsoft.utils.DateFormatUtils;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.NamingUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.text.ParseException;
import java.util.*;

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
        readFileToMap(recordFile, "utf-8", "\t").forEach(
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
        watch.start();

        //如果日期为空则从1970年1月1日开始抽取
        Date startTime = recordMap.get(NamingUtils.getOracleRecordKey(task));
        Date endTime;
        if (null == startTime) {
            Optional<Date> optional = dao.getMinTime("update_time");
            if (optional.isPresent())
                startTime = optional.get();
            else
                return;
        }
        //根据起始ID及步长从数据库里查询指定数据量的数据
        endTime = getEndTime(startTime, 30);

        //如果时间间隔不到30分钟不抽取（这类的数据太少）
        if (((new Date().getTime() - endTime.getTime()) / (30 * 60)) < 30)
            return;

        List<String[]> list = dao.getDataByTime(
                DateFormatUtils.DATE_TIME_FORMAT.format(startTime),
                DateFormatUtils.DATE_TIME_FORMAT.format(endTime)
        );
        logger.info("从 {} 取到到 {} 条数据", NamingUtils.getTableName(task), list.size());

        //根据不同的情况进行抽取
        extractDataOnCondition(list, task);

        //一次任务抽取完之后需要做的事情
        // extractTaskOver(task, id.get() + list.size());

    }

    /**
     * 从数据库中获取从指定时间开始最小的ID
     * 如果是内容的,比如说Bbs，Email，Ftp，Http，Imchat等从三个月前开始最小的ID
     * 如果是信息表的，比如说真实、虚拟、场所等没有时间限制，从最小的开始导入
     *
     * @param dao 数据库连接DAO层
     * @return 抽取起始ID
     */
    private static Optional<Long> getStartID(InfoDaoInter dao) {
        Optional<Long> id = Optional.empty();
        if (dao instanceof InfoDaoInter) {
            //最小的ID，没有时间限制
            id = dao.getMinId();
        }
        return id;
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
    public static void extractDataOnCondition(List<String[]> list, String task) {
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
    public static List<Integer> getKeyfieldIndexs(String[] fields, String[] keyfields) {
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
        Date endTime = DateUtils.addMinutes(startTime, minutes);
        if (((curTime.getTime() - endTime.getTime()) / (24 * 3600)) > 2) {
            endTime = DateUtils.addDays(startTime, 10);
        }
        return endTime;
    }
}
