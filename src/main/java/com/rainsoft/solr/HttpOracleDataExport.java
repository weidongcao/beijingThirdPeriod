package com.rainsoft.solr;

import com.rainsoft.dao.HttpDao;

import java.util.Optional;


/**
 * Oracle数据库Ftp数据导入Solr, HBase
 * Created by CaoWeiDong on 2017-06-28.
 */
public class HttpOracleDataExport extends BaseOracleDataExport {

    //任务类型
    private static final String task = "http";
    //dao
    private static HttpDao dao = (HttpDao) context.getBean("httpDao");

    /**
     * 按起始ID及步长将Oracle的数据导出到Solr、HBase
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
