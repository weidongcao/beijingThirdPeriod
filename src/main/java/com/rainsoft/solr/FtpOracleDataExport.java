package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.dao.FtpDao;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.HBaseUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Oracle数据库Ftp数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class FtpOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(FtpOracleDataExport.class);

    //ftpDao
    private static FtpDao ftpDao = (FtpDao) context.getBean("ftpDao");

    //字段名
    private static String[] columns = FieldConstants.COLUMN_MAP.get("oracle_reg_content_ftp");

    public static void ftpExportOracleByHours(Date startTime, JavaSparkContext sc) {
        String hourDate = BaseOracleDataExport.hourDateFormat.format(startTime);
        String contentType = BigDataConstants.CONTENT_TYPE_FTP;
        String ftpRecord = hourDate + "_" + contentType;
        //检查导入记录是是否有导入成功的记录，如果有跳过，如果没有的话再导入
        if (!BaseOracleDataExport.SUCCESS_STATUS.equals(BaseOracleDataExport.recordMap.get(ftpRecord))) {
            //记录导入开始时间
            long startIndexTime = new Date().getTime();

            //从Oracle抽数据的结束时间
            Date EndTime = DateUtils.addHours(startTime, hourOffset);

            //Oracle查询参数：开始时间
            String startTimeParam = TIME_FORMAT.format(startTime);
            //Oracle查询参数：结束时间
            String endTimeParam = TIME_FORMAT.format(EndTime);

            logger.info("contentType : 开始索引 {} 到 {} 的数据", startTimeParam, endTimeParam);
            //获取数据库指定捕获时间段的数据
            List<String[]> dataList = ftpDao.getFtpByHours(startTimeParam, endTimeParam);
            logger.info("从数据库查询数据结束");

            //导入Solr
            boolean flat = oracleContentTableDataExportSolr(dataList, contentType);
            //导入HBase
            ftpExportHBase(dataList, sc);


            //导入完成后对不同的结果的处理
            recordImportResult(contentType, hourDateFormat.format(startTime), flat);

            long endIndexTime = new Date().getTime();
            //计算索引一天的数据执行的时间（秒）
            long indexRunTime = (endIndexTime - startIndexTime) / 1000;
            logger.info("contentType 导出完成执行时间: {}分钟{}秒", indexRunTime / 60, indexRunTime % 60);
        } else {
            logger.info("{} : {} has already imported", hourDate, contentType);
        }
    }

    /**
     * Oracle的reg_content_ftp的数据导入HBase
     * @param list 数据列表
     * @param sc   JavaSparkContext
     */
    public static void ftpExportHBase(List<String[]> list, JavaSparkContext sc) {
        JavaRDD<String[]> javaRDD = sc.parallelize(list);
        /**
         * 将数据列表转为
         */
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = HBaseUtils.getHFileRDD(javaRDD, columns);
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, "H_REG_CONTENT_FTP", "CONTENT_FTP", BigDataConstants.TMP_HFILE_HDFS + "reg_content_ftp");
        logger.info("Oracle {} 数据写入HBase完成,数据量: {}", "reg_content_ftp", list.size());
        list.clear();
    }
}
