package com.rainsoft.solr.content;

import com.rainsoft.dao.HttpDao;
import com.rainsoft.solr.base.OracleContentDataExport;

import java.util.Optional;


/**
 * Oracle数据库Ftp数据导入Solr, HBase
 * Created by CaoWeiDong on 2017-06-28.
 */
public class HttpOracleDataExport {

    //任务类型
    private static final String task = "http";
    //dao
    private static HttpDao dao = (HttpDao) OracleContentDataExport.context.getBean("httpDao");

    /**
     * 按起始ID及步长将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        OracleContentDataExport.extract(task, dao, Optional.of(-3));
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }
}
