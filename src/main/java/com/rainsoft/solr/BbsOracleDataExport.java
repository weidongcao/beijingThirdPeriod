package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.dao.BbsDao;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Oracle数据库BBS数据导入Solr
 * Created by CaoWeiDong on 2017-10-15.
 */
public class BbsOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BbsOracleDataExport.class);

    //任务类型(bbs)
    private static final String task = BigDataConstants.CONTENT_TYPE_BBS;
    //HBase表名
    private static final String HBASE_TABLE_NAME = NamingRuleUtils.getHBaseTableName(task);

    //HBase列簇
    private static final String HBASE_TABLE_CF = NamingRuleUtils.getHBaseContentTableCF(task);

    //Oracle表名
    private static final String oracleTableName = NamingRuleUtils.getOracleContentTableName(task);

    //生成的HFile文件在HDFS的临时存储目录
    private static final String HFILE_TEMP_STORE_PATH = BigDataConstants.TMP_HFILE_HDFS + oracleTableName;

    //字段名
    private static String[] columns = FieldConstants.COLUMN_MAP.get(oracleTableName);

    private static BbsDao bbsDao = (BbsDao) context.getBean("bbsDao");

    private static final String recordKey = NamingRuleUtils.getRealTimeOracleRecordKey(task);

    public static void exportOracleByTime(Date startTime, boolean isImportHBase) {
        //监控执行情况
        StopWatch watch = new StopWatch();
        watch.start();

        //去掉开始时间的秒数
        startTime = DateUtils.truncate(startTime, Calendar.MINUTE);

        //Oracle查询参数：开始时间
        String startTimeParam = TIME_FORMAT.format(startTime);

        //从导入记录中获取最后导入的日期
        String lastExportRecord = recordMap.get(recordKey);

        Date endTime = null;
        try {
            endTime = DateUtils.parseDate(lastExportRecord, "yyyy-MM-dd HH:mm:ss");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        /*
         * 防止长时间没有导入,数据大量堆积的情况
         * 如果开始时间和记录的最后导入时间相距超过10天,取从最后导入时间之后10天的数据
         */
        Date tmpDate = DateUtils.addDays(endTime, 10);
        if (tmpDate.before(startTime)) {
            startTime = tmpDate;
        }

        logger.info("{} : 开始索引 {} 到 {} 的数据", task, startTimeParam, lastExportRecord);
        //获取数据库指定捕获时间段的数据
        List<String[]> dataList = bbsDao.getBbsByHours(startTimeParam, lastExportRecord);
        logger.info("从数据库查询数据结束,数据量: {}", dataList.size());

        if (dataList.size() > 0) {
            try {
                //根据数据量决定启动多少个线程
                int threadNum = dataList.size() / 200000 + 1;
                JavaRDD<String[]> javaRDD = getSparkContext().parallelize(dataList, threadNum);
                //数据持久化
                javaRDD.cache();

                //导入Solr
                oracleContentTableDataExportSolr(javaRDD, task);

                //导入HBase
                if (isImportHBase) {
                    exportHBase(javaRDD);
                }

                watch.stop();
                //记录任务执行时间
                logger.info("{} 导出完成。执行时间: {}", task, DurationFormatUtils.formatDuration(watch.getTime(), "yyyy-MM-dd HH:mm:ss"));

                //提供查询所导入数据的条件
                logger.info(
                        "Solr 查询此数据的条件: docType:{} capture_time:[{} TO {}]",
                        FieldConstants.DOC_TYPE_MAP.get(task),
                        startTime.getTime(),
                        endTime.getTime()
                );

                //导入记录写入文件
                String updateRecord = recordKey + "\t" + startTimeParam;
                writeRecordIntoFile(updateRecord);

                //导入记录写入Map
                recordMap.put(recordKey, startTimeParam);

                //一次任务结束程序休息一分钟
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            logger.info("Oracle数据库 {} 表在{} 至 {} 时间段内没有数据", "reg_content_bbs", startTimeParam, lastExportRecord);
        }
    }

    /**
     * Oracle的reg_content_ftp的数据导入HBase
     */
    private static void exportHBase(JavaRDD<String[]> javaRDD) {
        /**
         * 将数据列表转为
         */
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = HBaseUtils.getHFileRDD(javaRDD, columns);
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, HBASE_TABLE_NAME, HBASE_TABLE_CF, HFILE_TEMP_STORE_PATH);
        logger.info("Oracle {} 数据写入HBase完成", task);
    }

    public static void main(String[] args) throws ParseException {
        exportOracleByTime(TIME_FORMAT.parse("2017-09-26 00:00:00"), false);
    }
}
