package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.dao.ImchatDao;
import com.rainsoft.hbase.RowkeyColumnSecondarySort;
import com.rainsoft.utils.HBaseUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Oracle数据库Ftp数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ImchatOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(ImchatOracleDataExport.class);

    //imChatDao
    private static ImchatDao imChatDao = (ImchatDao) context.getBean("imchatDao");

    //字段名
    private static String[] columns = FieldConstants.COLUMN_MAP.get("oracle_reg_content_im_chat");

    public static void imchatExportOracleByHours(Date startTime) {
        String hourDate = BaseOracleDataExport.hourDateFormat.format(startTime);
        String contentType = BigDataConstants.CONTENT_TYPE_IM_CHAT;
        String imChatRecord = hourDate + "_" + contentType;
        //从Oracle抽数据的结束时间
        Date endTime = DateUtils.addHours(startTime, hourOffset);

        //Oracle查询参数：开始时间
        String startTimeParam = TIME_FORMAT.format(startTime);
        //Oracle查询参数：结束时间
        String endTimeParam = TIME_FORMAT.format(endTime);
        //检查导入记录是是否有导入成功的记录，如果有跳过，如果没有的话再导入
        if (!BaseOracleDataExport.SUCCESS_STATUS.equals(BaseOracleDataExport.recordMap.get(imChatRecord))) {
            //记录导入开始时间
            long startIndexTime = new Date().getTime();

            logger.info("{} : 开始索引 {} 到 {} 的数据", contentType, startTimeParam, endTimeParam);
            //获取数据库指定捕获时间段的数据
            List<String[]> dataList = imChatDao.getImChatByHours(startTimeParam, endTimeParam);
            logger.info("从数据库查询数据结束,数据量: {}", dataList.size());

            try {
                JavaRDD<String[]> javaRDD = getSparkContext().parallelize(dataList);
                javaRDD.cache();
                //导入Solr
                oracleContentTableDataExportSolr(javaRDD, contentType);
                //导入HBase
                imChatExportHBase(javaRDD);
                //记录导入结果
                updateRecordFile(contentType, hourDateFormat.format(startTime), true);
            } catch (Exception e) {
                updateRecordFile(contentType, hourDateFormat.format(startTime), false);
            }

            long endIndexTime = new Date().getTime();
            //计算索引一天的数据执行的时间（秒）
            long indexRunTime = (endIndexTime - startIndexTime) / 1000;
            logger.info("{} 导出完成执行时间: {}分钟{}秒", contentType, indexRunTime / 60, indexRunTime % 60);
            logger.info("Solr 查询此数据的条件: docType:聊天 capture_time:[{} TO {}]", startTime.getTime(), endTime.getTime());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            logger.info("{} 到 {} : {} 已经导入过了", startTimeParam, endTimeParam, contentType);
        }
    }

    /**
     * Oracle的reg_content_ftp的数据导入HBase
     *
     */
    public static void imChatExportHBase(JavaRDD<String[]> javaRDD) {
        /**
         * 将数据转为绕过二次排序的JavaPairRDD
         */
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = HBaseUtils.getHFileRDD(javaRDD, columns);
        //写入HBase
        HBaseUtils.writeData2HBase(hfileRDD, "H_REG_CONTENT_IM_CHAT", "CONTENT_IM_CHAT", "/tmp/hbase/hfile/reg_content_im_chat");
        logger.info("Oracle {} 数据写入HBase完成", "reg_content_im_chat");
    }

}
