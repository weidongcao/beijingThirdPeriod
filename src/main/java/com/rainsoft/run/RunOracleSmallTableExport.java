package com.rainsoft.run;

import com.rainsoft.solr.content.BbsOracleDataExport;
import com.rainsoft.solr.content.EmailOracleDataExport;
import com.rainsoft.solr.content.SearchOracleDataExport;
import com.rainsoft.solr.content.WeiboOracleDataExport;

/**
 * 导出Oracle内容表小表的数据
 * Created by CaoWeiDong on 2017-09-24.
 */
public class RunOracleSmallTableExport {

    public static void main(String[] args) {
        doJob();
    }

    private static void doJob() {
        while (true) {
            //Bbs任务
            BbsOracleDataExport.exportOracleByTime();

            //Email任务
            EmailOracleDataExport.exportOracleByTime();

            //Search任务
            SearchOracleDataExport.exportOracleByTime();

            //Weibo任务
            WeiboOracleDataExport.exportOracleByTime();

        }
    }
}
