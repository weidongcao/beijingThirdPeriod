package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.dao.BbsDao;
import com.rainsoft.dao.EmailDao;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.HBaseUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Oracle数据库BBS数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class EmailOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(EmailOracleDataExport.class);

    //任务类型(bbs)
    private static final String TASK_TYPE = BigDataConstants.CONTENT_TYPE_BBS;
    //HBase表名
    private static final String HBASE_TABLE_NAME = "H_REG_CONTENT_EMAIL";

    //HBase列簇
    private static final String HBASE_TABLE_CF = "CONTENT_EMAIL";

    //生成的HFile文件在HDFS的临时存储目录
    private static final String HFILE_TEMP_STORE_PATH = NamingRuleUtils.getHFileTaskDir(NamingRuleUtils.getOracleContentTableName("email"));

    //字段名
    private static String[] columns = FieldConstants.COLUMN_MAP.get("oracle_reg_content_email");

    private static EmailDao emailDao= (EmailDao) context.getBean("emailDao");

    public static void exportOracleByHours(Date startTime, Date endTime) {
        //Oracle查询参数：开始时间
        String startTimeParam = TIME_FORMAT.format(startTime);
        //Oracle查询参数：结束时间
        String endTimeParam = TIME_FORMAT.format(endTime);

        //记录导入开始时间
        long startIndexTime = new Date().getTime();

        logger.info("{} : 开始索引 {} 到 {} 的数据", TASK_TYPE, startTimeParam, endTimeParam);
        //获取数据库指定捕获时间段的数据
        List<String[]> dataList = emailDao.getEmailByHours(startTimeParam, endTimeParam);
        logger.info("从数据库查询数据结束,数据量: {}", dataList.size());

        try {
            JavaRDD<String[]> javaRDD = getSparkContext().parallelize(dataList);
            javaRDD.cache();
            //导入Solr
            oracleContentTableDataExportSolr(javaRDD, TASK_TYPE);
            //导入HBase
            EmailOracleDataExport.exportHBase(javaRDD);

            long endIndexTime = new Date().getTime();
            //计算任务执行的时间（秒）
            long indexRunTime = (endIndexTime - startIndexTime) / 1000;
            logger.info("{} 导出完成执行时间: {}分钟{}秒", TASK_TYPE, indexRunTime / 60, indexRunTime % 60);
            logger.info(
                    "Solr 查询此数据的条件: docType:{} capture_time:[{} TO {}]",
                    FieldConstants.DOC_TYPE_MAP.get(TASK_TYPE),
                    startTime.getTime(),
                    endTime.getTime()
            );
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
        logger.info("Oracle {} 数据写入HBase完成", TASK_TYPE);
    }

}
