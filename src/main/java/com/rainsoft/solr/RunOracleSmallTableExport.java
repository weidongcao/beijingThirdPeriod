package com.rainsoft.solr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
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

        }
    }
}
