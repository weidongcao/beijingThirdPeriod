package com.rainsoft.solr.distinct;

import com.rainsoft.dao.ImsiDao;
import com.rainsoft.solr.base.OracleDistinctDataExport;

/**
 * Oracle数据库 imsi 数据导入Solr
 * Created by CaoWeiDong on 2018-04-24 11:22:20
 */
public class ImsiOracleDataExport {

    private static ImsiDao dao = (ImsiDao) OracleDistinctDataExport.context.getBean("imsiDao");

    private static final String task = "imsi";

    /**
     * 按时间将Oracle的数据导出到Solr、HBase
     */
    public static void exportOracleByTime() {
        OracleDistinctDataExport.extract(task, dao);
    }

    public static void main(String[] args) {
        do {
            exportOracleByTime();
        } while (true);
    }

}
