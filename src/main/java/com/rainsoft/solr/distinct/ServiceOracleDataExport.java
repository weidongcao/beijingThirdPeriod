package com.rainsoft.solr.distinct;

import com.rainsoft.dao.ServiceDao;
import com.rainsoft.solr.base.OracleDistinctDataExport;

/**
 * Oracle数据库 场所 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ServiceOracleDataExport {

    private static ServiceDao dao = (ServiceDao) OracleDistinctDataExport.context.getBean("serviceDao");

    private static final String task = "service";

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
