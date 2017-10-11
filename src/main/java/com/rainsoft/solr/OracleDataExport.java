package com.rainsoft.solr;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by CaoWeiDong on 2017-09-24.
 */
public class OracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(OracleDataExport.class);
    private static int syncTime = ConfigurationManager.getInteger("sync.time");

    public static void main(String[] args) {
        //开始时间参数
        String startString = args[0];
        //结束时间参数
        String endString = args[1];
        String taskType = "";
        if (args.length == 3) {
            try {
                taskType = args[2];
            } catch (Exception e) {
                logger.error("类型参数异常");
            }
        }
        //开始时间
        Date startTime = null;
        //结束时间
        Date endTime = null;

        try {
            startTime = BaseOracleDataExport.dateDateFormat.parse(startString);
            endTime = BaseOracleDataExport.dateDateFormat.parse(endString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        startTime = DateUtils.addHours(startTime, -BaseOracleDataExport.hourOffset);

        logger.info("导入开始时间: {}", BaseOracleDataExport.TIME_FORMAT.format(startTime));
        logger.info("导入结束时间: {}", BaseOracleDataExport.TIME_FORMAT.format(endTime));

        while (startTime.after(endTime)) {
            if (taskType.equalsIgnoreCase("ftp")) {
                //迁移Ftp表的历史数据
                FtpOracleDataExport.ftpExportOracleByHours(startTime);
                waitForSync();
            } else if (taskType.equalsIgnoreCase("im_chat")) {
                //迁移聊天表的历史数据
                ImchatOracleDataExport.imchatExportOracleByHours(startTime);
                waitForSync();
            } else if (taskType.equalsIgnoreCase("http")) {
                //迁移网页表的历史数据
                HttpOracleDataExport.httpExportOracleByHours(startTime);
                waitForSync();
            } else {
                //迁移Ftp表的历史数据
                FtpOracleDataExport.ftpExportOracleByHours(startTime);
                waitForSync();

                //迁移聊天表的历史数据
                ImchatOracleDataExport.imchatExportOracleByHours(startTime);
                waitForSync();

                //迁移网页表的历史数据
                HttpOracleDataExport.httpExportOracleByHours(startTime);
                waitForSync();
            }

            //时间向前推进
            startTime = DateUtils.addHours(startTime, -BaseOracleDataExport.hourOffset);
            //获取时间的小数数
            int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
            //白天休息不迁移历史数据
            if (hour >= 6 && hour < 22) {
                break;
            }

        }
        logger.info("程序执行结束,马上退出");

    }

    public static void waitForSync() {
        try {
            Thread.sleep(1000 * 60 * syncTime);
            logger.info("等待Solr数据同步完成...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
