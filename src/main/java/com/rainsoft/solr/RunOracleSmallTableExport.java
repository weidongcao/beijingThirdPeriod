package com.rainsoft.solr;

/**
 * 导出Oracle内容表小表的数据
 * Created by CaoWeiDong on 2017-09-24.
 */
public class RunOracleSmallTableExport {

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
