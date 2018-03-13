package com.rainsoft.solr;

import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;

/**
 * 导出Oracle表数据,所有的表包括：
 * 迁移Ftp表的历史数据
 * 迁移im_chat表的历史数据
 * 迁移http表的历史数据
 * 迁移bbs表的历史数据
 * 迁移email表的历史数据
 * 迁移search表的历史数据
 * 迁移real表的历史数据
 * 迁移vid表的历史数据
 * 迁移real表的历史数据
 * Created by CaoWeiDong on 2017-10-23.
 */
public class RunOracleAllTableExport {
    private static final Logger logger = LoggerFactory.getLogger(RunOracleAllTableExport.class);

    public static void main(String[] args) {
        doJob(args);
    }

    public static void doJob(String[] args) {
        //结束时间参数
        String endTime_String = null;
        if (args.length >= 1) {
            try {
                endTime_String = args[0];
            } catch (Exception e) {
                logger.error("类型参数异常");
            }
        }
        //结束时间
        Date endTime = null;

        if (null != endTime_String) {
            logger.info("导入结束时间: {}", endTime_String);
            try {
                endTime = DateUtils.parseDate(endTime_String, "yyyy-MM-dd HH:mm:ss");
            } catch (ParseException e) {
                logger.error("给定的日期格式不正确正确的格式为:yyyy-MM-dd HH:mm:ss");
                System.exit(-1);
            }
        }

        while (true) {
            //迁移Ftp表的历史数据
            FtpOracleDataExport.exportOracleByTime();

            //迁移聊天表的历史数据
            ImchatOracleDataExport.exportOracleByTime();

            //迁移网页表的历史数据
            HttpOracleDataExport.exportOracleByTime();

            //Bbs任务
            BbsOracleDataExport.exportOracleByTime();

            //Email任务
            EmailOracleDataExport.exportOracleByTime();

            //Search任务
            SearchOracleDataExport.exportOracleByTime();

            //Weibo任务
            WeiboOracleDataExport.exportOracleByTime();

            //真实表任务
            RealOracleDataExport.exportOracleByTime();

            //虚拟表任务
            VidOracleDataExport.exportOracleByTime();

        }
    }
}
