package com.rainsoft.solr;

import com.rainsoft.dao.ServiceDao;

import java.util.Optional;

/**
 * Oracle数据库 场所 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ServiceOracleDataExport extends BaseOracleDataExport {

    private static ServiceDao dao = (ServiceDao) context.getBean("serviceDao");

    private static final String task = "service";

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
