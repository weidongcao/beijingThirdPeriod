package com.rainsoft.run;

import com.rainsoft.solr.content.*;

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

    public static void main(String[] args) {
        doJob();
    }

    private static void doJob() {
        do {
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
            // RealOracleDataExport.exportOracleByTime();

            //虚拟表任务
            // VidOracleDataExport.exportOracleByTime();

        } while (true);
    }
}
