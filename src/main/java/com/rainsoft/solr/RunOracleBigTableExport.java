package com.rainsoft.solr;

import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * 导出Oracle内容表大表的数据包括：
 * 导出FTP的数据
 * 导出IM_chat的数据
 * 导出HTTP的数据
 * Created by CaoWeiDong on 2017-09-24.
 */
public class RunOracleBigTableExport {
    private static final Logger logger = LoggerFactory.getLogger(RunOracleBigTableExport.class);

    public static void main(String[] args) {
        //结束时间参数
        String endTime_String = null;
        if (args.length == 1) {
            try {
                endTime_String = args[0];
            } catch (Exception e) {
                logger.error("类型参数异常");
            }
        }
        //结束时间
        Date endTime = endTime_String == null ? null : DateUtils.stringToDate(endTime_String, "yyyy-MM-dd HH:mm:ss");

        if (null != endTime_String) {
            logger.info("导入结束时间: {}", endTime_String);
        }

        while (true) {
            //如果超过结束时间结束任务
            if (null != endTime) {
                Date lastRecordDate = DateUtils.stringToDate(BaseOracleDataExport.recordMap.get(NamingRuleUtils.getRealTimeOracleRecordKey("http")), "yyyy-MM-dd HH:mm:ss");
                if (endTime.before(lastRecordDate)) {
                    break;
                }
                //获取时间的小数数
//                int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
                //白天休息不迁移历史数据
//                if (hour >= 6 && hour < 22) {
//                    break;
//                }
            }
            //迁移Ftp表的历史数据
            FtpOracleDataExport.exportOracleByTime();

            //迁移聊天表的历史数据
            ImchatOracleDataExport.exportOracleByTime();

            //迁移网页表的历史数据
            HttpOracleDataExport.exportOracleByTime();

        }
        logger.info("程序执行结束,马上退出");

    }
}
