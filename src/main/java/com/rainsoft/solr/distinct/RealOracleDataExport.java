package com.rainsoft.solr.distinct;

import com.rainsoft.dao.RealDao;
import com.rainsoft.solr.base.OracleDistinctDataExport;

/**
 * Oracle数据库 真实 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RealOracleDataExport {

    private static RealDao dao = (RealDao) OracleDistinctDataExport.context.getBean("realDao");

    private static final String task = "real";

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
