package com.rainsoft.solr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 导出Oracle内容表小表的数据
 * Created by CaoWeiDong on 2017-09-24.
 */
public class RunOracleSmallTableExport {
    private static final Logger logger = LoggerFactory.getLogger(RunOracleSmallTableExport.class);

    public static void main(String[] args) {

        while (true) {
            //Bbs任务
            BbsOracleDataExport.exportOracleByTime();

            //Email任务
            EmailOracleDataExport.exportOracleByTime();

            //Search任务
            SearchOracleDataExport.exportOracleByTime();

            //Weibo任务
            WeiboOracleDataExport.exportOracleByTime();

            //Shop任务
//            ShopOracleDataExport.exportOracleByTime();

            //真实表任务
            RealOracleDataExport.exportOracleByTime();

            //虚拟表任务
            VidOracleDataExport.exportOracleByTime();

        }
    }
}
