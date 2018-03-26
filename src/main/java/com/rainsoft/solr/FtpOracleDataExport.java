package com.rainsoft.solr;

import com.rainsoft.dao.FtpDao;

import java.util.Optional;

/**
 * Oracle数据库Ftp数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class FtpOracleDataExport extends BaseOracleDataExport {

    private static final String task = "ftp";
    //dao
    private static FtpDao dao = (FtpDao) context.getBean("ftpDao");

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        extract(task, dao, Optional.of(-3));
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }
}
