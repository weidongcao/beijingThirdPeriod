package com.rainsoft.solr;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.dao.BbsDao;
import com.rainsoft.utils.NamingRuleUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * Oracle数据库BBS数据导入Solr和HBase
 * Created by CaoWeiDong on 2017-10-15.
 */
public class BbsOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(BbsOracleDataExport.class);

    //任务类型(bbs)
    private static final String task = BigDataConstants.CONTENT_TYPE_BBS;


    private static BbsDao dao = (BbsDao) context.getBean("bbsDao");

    public static void exportOracleByTime(Date curTime, boolean isImportHBase) {
        //监控执行情况
        watch.start();

        //根据当前时间和任务类型获取要从Oracle查询的开始时间和结束时间
        Tuple2<String, String> period = getPeriod(curTime, task, 240);
        logger.info("{} : 开始索引 {} 到 {} 的数据", task, period._1, period._2);

        //获取数据库指定捕获时间段的数据
        List<String[]> dataList = dao.getBbsByHours(period._1, period._2);
        logger.info("从数据库查询数据结束,数据量: {}", dataList.size());

        if (dataList.size() > 0) {
            //数据导出到Solr、HBase
            exportData(dataList, task, isImportHBase);

            //导入记录写入Map
            recordMap.put(NamingRuleUtils.getRealTimeOracleRecordKey(task), period._2());
            //导入记录写入文件
            overwriteRecordFile();

            //停止计时
            watch.stop();

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
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            //停止计时
            watch.stop();
            logger.info("Oracle数据库 {} 表在{} 至 {} 时间段内没有数据", "reg_content_bbs", period._1, period._2);
        }
        //重置计时器
        watch.reset();
    }

    public static void main(String[] args) throws ParseException {
        exportOracleByTime(TIME_FORMAT.parse("2017-10-17 00:00:00"), true);
    }
}
