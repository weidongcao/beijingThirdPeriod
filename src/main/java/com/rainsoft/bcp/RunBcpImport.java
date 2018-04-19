package com.rainsoft.bcp;

import com.rainsoft.solr.content.BbsOracleDataExport;
import com.rainsoft.solr.content.EmailOracleDataExport;
import com.rainsoft.solr.content.SearchOracleDataExport;
import com.rainsoft.solr.content.WeiboOracleDataExport;
import com.rainsoft.utils.ThreadUtils;

import java.util.Calendar;

/**
 * Created by CaoWeiDong on 2018-01-31.
 */
public class RunBcpImport {
    public static final String[] bigTasks = new String[]{"ftp", "im_chat", "http"};
    public static final String[] smallTasks = new String[]{"bbs", "email", "search", "weibo",};

    public static void main(String[] args) {
        while (true) {
            runBigTasks(bigTasks);
            runSmallTasks();
        }
    }

    /**
     * 执行大任务
     * 大任务一般为HTTP、FTP、imchat
     * 如果大任务都没有数据,休息5分钟
     * @param bigTasks
     */
    public static void runBigTasks(String[] bigTasks) {
        //统计没有数据的任务
        int noDataTaskCount = 0;
        //执行任务
        for (String task : bigTasks) {
            String status = BaseBcpImportHBaseSolr.doTask(task);
            if ("none".equals(status)) {
                noDataTaskCount++;
            }
        }
        //如果三个类型都没有数据,休息10分钟
        if (noDataTaskCount == bigTasks.length) {
            ThreadUtils.programSleep(60 * 5);
        }
    }

    /**
     * 执行小任务
     * 一般为Bbs、Email、Search、Real、Vid、Service
     * 因为数据量小,每4个小时执行一次
     */
    public static void runSmallTasks() {
        Calendar calendar = Calendar.getInstance();
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        if (hour % 6 == 0) {
            //Bbs任务
            BbsOracleDataExport.exportOracleByTime();

            //Email任务
            EmailOracleDataExport.exportOracleByTime();

            //Search任务
            SearchOracleDataExport.exportOracleByTime();

            //Weibo任务
            WeiboOracleDataExport.exportOracleByTime();

            //虚拟表任务
            // VidOracleDataExport.exportOracleByTime();

            //真实表任务
            // RealOracleDataExport.exportOracleByTime();
        }
    }
}
